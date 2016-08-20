package com.kryptnostic.sparks;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.spark.SparkConf;
import org.apache.spark.deploy.master.LeaderElectable;
import org.apache.spark.serializer.Serializer;
import org.junit.AfterClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.kryptnostic.sparks.HazelcastRecoveryModeFactory.LeaderElection;

public class HazelcastRecoveryModeTester {
    static final HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance();
    static final ExecutorService   executor  = Executors.newFixedThreadPool( 3 );

    @Test(
        expected = IllegalArgumentException.class )
    public void testBlankSparkConf() {
        SparkConf conf = new SparkConf();
        conf.set( HazelcastRecoveryModeFactory.HAZELCAST_NODES_KEY, "" );
        new HazelcastRecoveryModeFactory( conf, Mockito.mock( Serializer.class ) );
    }

    @Test
    public void testLeaderElection() throws InterruptedException {
        SparkConf conf = new SparkConf();
        HazelcastRecoveryModeFactory.registerCustomRecoveryModeInSparkConf( conf );
        InetSocketAddress sockAddr = hazelcast.getCluster().getLocalMember().getSocketAddress();
        String nodes = sockAddr.getAddress().getHostAddress() + ":" + sockAddr.getPort();
        Serializer serializer = Mockito.mock( Serializer.class );
        final CountDownLatch latch = new CountDownLatch( 1 );
        MessageListener<LeaderElection> electionListener = new MessageListener<LeaderElection>() {
            @Override
            public void onMessage( Message<LeaderElection> message ) {
                latch.countDown();
            }
        };

        hazelcast.<LeaderElection> getReliableTopic( HazelcastRecoveryModeFactory.LEADER_ELECTION_HZ_NAME )
                .addMessageListener( electionListener );

        conf.set( HazelcastRecoveryModeFactory.HAZELCAST_NODES_KEY, nodes );

        List<HazelcastRecoveryModeFactory> factories = ImmutableList.of(
                new HazelcastRecoveryModeFactory(
                        conf,
                        serializer ),
                new HazelcastRecoveryModeFactory(
                        conf,
                        serializer ),
                new HazelcastRecoveryModeFactory(
                        conf,
                        serializer ) );

        final CountDownLatch threads = new CountDownLatch( factories.size() );
        LeaderElectable electable = Mockito.mock( LeaderElectable.class );
        factories.forEach( factory -> executor.submit( () -> {
            factory.createLeaderElectionAgent( electable );
            threads.countDown();
        } ) );

        threads.await();
        latch.await();
        
        Mockito.verify( electable, Mockito.times( 1 ) ).electedLeader();
    }

    @AfterClass
    public static void shutdownHazelcast() {
        hazelcast.shutdown();
    }
}
