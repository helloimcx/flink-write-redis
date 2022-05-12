package com.sample.bean;

import java.util.ArrayList;

import static com.sample.util.RandomNum.getRandInt;

public class User {
    int id;
    int age;
    int gender;
    int city;
    int device;

    public User() {
        id = getRandInt(1, 100 * 10000);
        age = id % 100 + 1;
        gender = id % 2;
        city = id % 49 + 1;
        device = id % 20 + 1;
    }

    @Override
    public String toString() {
        return id + "#" + age + "#" + gender + "#" + city + "#" + device;
    }
}
