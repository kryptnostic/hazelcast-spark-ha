package com.kryptnostic.sparks;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.master.LeaderElectable;
import org.apache.spark.deploy.master.LeaderElectionAgent;
import org.apache.spark.deploy.master.PersistenceEngine;
import org.apache.spark.deploy.master.StandaloneRecoveryModeFactory;
import org.apache.spark.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.util.Preconditions;
import com.google.common.base.Splitter;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

import scala.collection.Seq;
import scala.reflect.ClassTag;

/**
 * Spark that adds high availability support through Hazelcast. In addition, if a mapstore is registered for the
 * {@link HazelcastRecoveryModeFactory#SPARK_RECOVERY_MAP} it enables persistence of data to permanent datastores like
 * Cassandra.
 * 
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 *
 */
public class HazelcastRecoveryModeFactory extends StandaloneRecoveryModeFactory {
    /**
     * This is the name of the map will be used to store failover information for Spark master.
     */
    public static final String      LEADER_ELECTION_HZ_NAME = "_leader-election";
    public static final String      SPARK_RECOVERY_MAP      = "_spark_recovery";
    public static final String      HAZELCAST_NODES_KEY     = "com.kryptnostic.hazelcast.nodes";
    private final HazelcastInstance hazelcast;

    public HazelcastRecoveryModeFactory( SparkConf conf, Serializer serializer ) {
        super( conf, serializer );
        String csNodes = conf.get( HAZELCAST_NODES_KEY );
        Preconditions.checkArgument( StringUtils.isNotBlank( csNodes ), "Hazelcast nodes must be provided." );
        List<String> nodes = Splitter.on( "," ).trimResults().omitEmptyStrings().splitToList( csNodes );
        this.hazelcast = HazelcastClient.newHazelcastClient(
                new ClientConfig().setNetworkConfig( new ClientNetworkConfig().setAddresses( nodes ) ) );
    }

    @Override
    public LeaderElectionAgent createLeaderElectionAgent( LeaderElectable masterInstance ) {
        return new HazelcastLeaderElectionAgent( masterInstance, hazelcast );
    }

    @Override
    public PersistenceEngine createPersistenceEngine() {
        return new HazelcastPersistenceEngine( hazelcast );
    }

    public static void registerCustomRecoveryModeInSparkConf( SparkConf conf ) {
        conf.set( "spark.deploy.recoveryMode", "CUSTOM" );
        conf.set( "spark.deploy.recoveryMode.factory", HazelcastRecoveryModeFactory.class.getCanonicalName() );
    }

    public static class HazelcastPersistenceEngine extends PersistenceEngine {
        private final IMap<String, Object> store;

        public HazelcastPersistenceEngine( HazelcastInstance hazelcastInstance ) {
            store = hazelcastInstance.getMap( SPARK_RECOVERY_MAP );
        }

        @Override
        public void persist( String key, Object obj ) {
            store.put( key, obj );
        }

        @SuppressWarnings( "unchecked" )
        @Override
        public <T> Seq<T> read( String key, ClassTag<T> clazz ) {
            return (Seq<T>) store.get( key );
        }

        @Override
        public void unpersist( String key ) {
            store.delete( key );
        }
    }

    public static class LeaderElection implements Serializable {
        private static final long          serialVersionUID = 2176378639581806212L;
        public static final LeaderElection SINGLETON        = new LeaderElection();
    }

    public static class HazelcastLeaderElectionAgent
            implements LeaderElectionAgent, LifecycleListener, MessageListener<LeaderElection> {
        private static final Logger          logger   = LoggerFactory
                .getLogger( HazelcastLeaderElectionAgent.class );
        private final LeaderElectable        masterInstance;
        private final ILock                  election;
        private final ITopic<LeaderElection> electionTopic;
        private final ExecutorService        executor = Executors.newFixedThreadPool( 4 );
        private final CountDownLatch         block    = new CountDownLatch( 1 );

        public HazelcastLeaderElectionAgent( LeaderElectable masterInstance, HazelcastInstance hazelcast ) {
            this.masterInstance = masterInstance;
            this.electionTopic = hazelcast.getReliableTopic( LEADER_ELECTION_HZ_NAME );
            this.election = hazelcast.getLock( LEADER_ELECTION_HZ_NAME );
            this.electionTopic.addMessageListener( this );
            electLeaderAsync();
        }

        public void electLeaderAsync() {
            executor.execute( () -> {
                logger.info( "Starting leadership election using Hazelcast Lock." );
                election.lock();
                masterInstance.electedLeader();
                electionTopic.publish( LeaderElection.SINGLETON );
            } );

            boolean electionComplete = false;
            try {
                electionComplete = block.await( 1, TimeUnit.SECONDS );
            } catch ( InterruptedException e ) {
                logger.error(
                        "Election interrupted on this node. Assuming election was completed successfully and continuing." );
                return;
            }

            if ( !electionComplete ) {
                logger.error(
                        "Election completion notification not received by this node. Assuming election was completed successfully and continuing." );
            }
        }

        @Override
        public LeaderElectable masterInstance() {
            return masterInstance;
        }

        @Override
        public void stop() {
            executor.shutdown();
        }

        @Override
        public void stateChanged( LifecycleEvent event ) {
            if ( event.getState() == LifecycleState.CLIENT_DISCONNECTED ) {
                masterInstance.revokedLeadership();
            }
        }

        @Override
        public void onMessage( Message<LeaderElection> message ) {
            final InetSocketAddress address = message.getPublishingMember().getSocketAddress();
            executor.execute( () -> {
                logger.info( "Elected leader: {} @ {}:{}",
                        address.getHostName(),
                        address.getAddress().getHostAddress(),
                        address.getPort() );
                block.countDown();
            } );
        }
    }

}
