/*
 * Course: High Performance Computing 2023/2024
 *
 * Lecturer:    D'Aniello   Giuseppe    gidaniello@unisa.it
 *
 * Student:
 * Pepe Lorenzo        0622702121      l.pepe29@studenti.unisa.it
 *
 * Copyright (C) 2023 - All Rights Reserved
 *
 * This file is part of Final Project HPC.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with ContestOMP.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package it.unisa.diem.BigData;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class SparkDriver {
    public static void main(String[] args){
        String inputPath = args[0];
        String outputPath = args[1];
        //int k = Integer.parseInt(args[2]);

        SparkConf conf = new SparkConf().setAppName("Spark Outlet");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> OutletRDD = sc.textFile(inputPath);

        JavaPairRDD<Tuple2<String, String>, Double> mappedOutletRDD = OutletRDD.mapToPair(line -> {
                    String[] fields = line.split(",");
                    String outletId = fields[6];
                    String outletSize = fields[8];
                    double MRP = Double.parseDouble(fields[5]);
                    return new Tuple2<>(new Tuple2<>(outletId, outletSize), MRP);
                }).filter(tuple -> !tuple._1()._2().isEmpty());

        JavaPairRDD<Tuple2<String, String>, Tuple2<Double, Integer>> sumRDD = mappedOutletRDD
                .mapValues(mrp -> new Tuple2<>(mrp, 1))
                .reduceByKey((a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2()));

        JavaPairRDD<Tuple2<String, String>, Double> meanMRP = sumRDD
                .mapToPair(tuple -> new Tuple2<>(
                        tuple._1(),
                        tuple._2()._1() / tuple._2()._2()
                ));

        JavaPairRDD<String, Tuple2<String, Double>> mapBySize = meanMRP
                .mapToPair(tuple -> new Tuple2<>(tuple._1()._2(), new Tuple2<>(tuple._1()._1(), tuple._2())));

        JavaPairRDD<String, Iterable<Tuple2<String, Double>>> groupBySize = mapBySize.groupByKey();

        JavaPairRDD<String, List<Tuple2<String, Double>>> topKStoresBySize = groupBySize
                .mapValues(iterable -> {
                    List<Tuple2<String, Double>> list = new ArrayList<>();
                    for (Tuple2<String, Double> item : iterable) {
                        list.add(item);
                    }

                    return list.stream()
                            .sorted(Comparator.comparingDouble(Tuple2::_2)) // Sort by MRP ascending
                            .limit(3)
                            .collect(Collectors.toList());
                });

        JavaRDD<String> sortedOutputRDD = topKStoresBySize.flatMap(entry -> {
            String outletSize = entry._1();
            List<Tuple2<String, Double>> stores = entry._2();

            List<String> resultLines = new ArrayList<>();
            for (Tuple2<String, Double> store : stores) {
                resultLines.add("Outlet Size: " + outletSize + ", Store: " + store._1() + ", Avg MRP: " + store._2());
            }
            return resultLines.iterator();
        });

        sortedOutputRDD.saveAsTextFile(outputPath);

        sc.close();
    }
}
