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

import java.util.Comparator;
import java.util.Objects;
public class LFComparator implements Comparator<MostLowFatProduct>{

    @Override
    public int compare(MostLowFatProduct LF1, MostLowFatProduct LF2){

        if(LF2.LFProduct.equals(LF1.LFProduct)){
            if(LF2.store_ID.equals(LF1.store_ID)){
                return Objects.compare(LF2, LF1, null);
            }else{
                return LF2.store_ID.compareToIgnoreCase(LF1.store_ID);
            }
        }
        return LF2.LFProduct.compareTo(LF1.LFProduct);
    }
}
