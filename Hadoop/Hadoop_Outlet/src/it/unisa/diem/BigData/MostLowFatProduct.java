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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
public class MostLowFatProduct implements Writable {
    String store_ID;
    Integer LFProduct;
    public MostLowFatProduct() {
        this.store_ID = "";
        this.LFProduct = 0;
    }
    public MostLowFatProduct(String store_ID, Integer LFProduct) {
        this.store_ID = store_ID;
        this.LFProduct = LFProduct;
    }
    public String getStore_ID() {
        return store_ID;
    }
    public void setStore_ID(String store_ID) {
        this.store_ID = store_ID;
    }
    public Integer getLFProduct() {
        return LFProduct;
    }
    public void setLFProduct(Integer LFProduct) {
        this.LFProduct = LFProduct;
    }
    @Override
    public void readFields(DataInput input) throws IOException{
        store_ID = input.readUTF();
        LFProduct = input.readInt();
    }
    @Override
    public void write(DataOutput output) throws IOException{
        output.writeUTF(store_ID);
        output.writeInt(LFProduct);
    }
    @Override
    public String toString(){
        return "Store_ID: "+store_ID+", Low_Fat_Product: "+LFProduct;
    }
}
