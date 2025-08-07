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
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
public class SecondMapper extends Mapper<
        Text,                                   // Input key type
        Text,                                   // Input value type
        NullWritable,                           // Output key type
        MostLowFatProduct                       // Output value type
>{
    private List<MostLowFatProduct> topStore;
    protected void setup(Context context){
        topStore = new LinkedList<MostLowFatProduct>();
    }
    protected void map(
            Text key,                           // Input key type
            Text value,                         // Input value type
            Context context) throws IOException, InterruptedException
    {
        String Store = key.toString();
        Integer LFProduct = Integer.parseInt(value.toString());
        topStore.add(new MostLowFatProduct(Store, LFProduct));
        topStore.sort(new LFComparator());
        if(topStore.size() > 5){
            topStore.subList(5, topStore.size()).clear();
        }
    }
    protected void cleanup(Context context) throws IOException, InterruptedException{
        for(MostLowFatProduct LF: topStore){
            context.write(NullWritable.get(), LF);
        }
    }
}