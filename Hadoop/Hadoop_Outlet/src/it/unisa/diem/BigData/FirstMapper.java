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
import org.apache.hadoop.mapreduce.Mapper;
public class FirstMapper extends Mapper<
        Text, 		                                    // Input key type
        Text, 		                                    // Input value type
        Text,                                           // Output key type
        IntWritable>                                    // Output value type
{
    private final static IntWritable one = new IntWritable(1);
    protected void map(
            Text key,   	                            //Input key
            Text value,                             // Input value
            Context context) throws IOException, InterruptedException {
        // get the entire content of the line in an array
        String[] fields = key.toString().split(",");
        // get the relevant information on the outlet and on the specific product
        String Item_Fat_Content = fields[2];
        // Check only for "Lof Fat" item
        if (Item_Fat_Content.equals("Low Fat"))
        {
            // emit the pair (Unique_ID 1)
            context.write(new Text(fields[6]), one);
        }
    }
}