package com.sample.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import static com.sample.util.RandomNum.getRandInt;


public class Item {
    int id;
    int price;

    public Item() {
        id = getRandInt(1, 1000);
        price = id % 100 + 1;
    }

    @Override
    public String toString() {
        return id + "#" + price;
    }
}
