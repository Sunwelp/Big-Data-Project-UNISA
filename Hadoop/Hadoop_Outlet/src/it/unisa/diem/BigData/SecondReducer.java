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
import org.apache.hadoop.mapreduce.Reducer;

public class SecondReducer extends Reducer<NullWritable, MostLowFatProduct, NullWritable, MostLowFatProduct>{
    protected void reduce(NullWritable key,                     // Input key type
                          Iterable<MostLowFatProduct>  values,  // Input value type
                          Context context) throws IOException, InterruptedException {
        List<MostLowFatProduct> TopLFPStore = new LinkedList<MostLowFatProduct>();
        // Write only the Top-K found in the new dataset (linked list in this case)
        for (MostLowFatProduct value : values) {
            TopLFPStore.add(new MostLowFatProduct(value.getStore_ID(), value.getLFProduct()));
        }
        TopLFPStore.sort(new LFComparator());
        if(TopLFPStore.size() > 5){
            TopLFPStore.subList(5, TopLFPStore.size()).clear();
        }
        for(MostLowFatProduct LFP : TopLFPStore){
            context.write(NullWritable.get(), LFP);
        }
    }
}
