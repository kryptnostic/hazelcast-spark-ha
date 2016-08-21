package com.kryptnostic.sparks;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class EclipseBugRepro {

    public static void main( String [] args ) {
        SparkConf conf = new SparkConf().setAppName( "Kryptnostic Spark Datastore" )
                .setMaster( "spark://mjolnir.local:7077" )
                .setJars( new String[] { "./build/libs/hazelcast-spark-ha-0.0.0.jar" });
        JavaSparkContext spark = new JavaSparkContext( conf );
        JavaRDD<String> s = spark.textFile( "src/test/resources/test.csv" );
        s.foreach(  l -> System.out.println( l ) );
    }
}
