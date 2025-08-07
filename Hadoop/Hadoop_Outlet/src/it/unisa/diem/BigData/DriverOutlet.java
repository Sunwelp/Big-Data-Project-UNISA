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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DriverOutlet {
    public static void main(String[] args) throws Exception {

        Path inputPath;
        Path outputDir;
        Path outputDirStep2;
        int numberOfReducers;

        // Parse the parameters
        numberOfReducers = Integer.parseInt(args[0]);
        inputPath = new Path(args[1]);
        outputDir = new Path(args[2]);
        outputDirStep2 = new Path(args[3]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("Outlet analisys - Step 1");
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setJarByClass(DriverOutlet.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(FirstMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(FirstReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(numberOfReducers);
        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2);
        job2.setJobName("Outlet analisys - Step 2");
        FileInputFormat.addInputPath(job2, outputDir);
        FileOutputFormat.setOutputPath(job2, outputDirStep2);
        job2.setJarByClass(DriverOutlet.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setMapperClass(SecondMapper.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(MostLowFatProduct.class);
        job2.setReducerClass(SecondReducer.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(MostLowFatProduct.class);
        job2.setNumReduceTasks(1);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
