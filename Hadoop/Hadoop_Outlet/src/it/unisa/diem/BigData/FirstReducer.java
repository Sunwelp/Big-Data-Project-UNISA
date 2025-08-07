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
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class FirstReducer extends Reducer< Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key,                     // Input key type
                          Iterable<IntWritable> values, // Input value type
                          Context context) throws IOException, InterruptedException {
        int LowFatProduct = 0;
        // Iterate over the set of values and sum them
        for (IntWritable value : values) {
            LowFatProduct = LowFatProduct + value.get();
        }
        context.write(key, new IntWritable(LowFatProduct));
    }
}