package com.sample.bean;


import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

import static com.sample.util.RandomNum.getRandInt;

@Data
public class Event {
    List<Item> items;
    User user;
    long timestamp;
    int label;
    int type; // 渠道

    public Event(int label) { // 曝光
        this.label = label;
        items = new ArrayList<>();
        for (int i = 0; i < getRandInt(5, 10); ++i) {
            items.add(new Item());
        }
        user = new User();
        timestamp = System.currentTimeMillis() / 1000;
        type = (int) (System.currentTimeMillis() % 5);
    }

    public Event(int label, Event popEvent) { // 购买
        this.label = label;
        this.items = new ArrayList<>();
        this.items.add(popEvent.getItems().get(0));
        this.user = popEvent.user;
        this.timestamp = System.currentTimeMillis() / 1000;
        this.type = popEvent.type;
    }

    @Override
    public String toString() {
        return items + "#" + user + "#" + timestamp + "#" + label + "#" + type;
    }

    public Row toRow() {
        return Row.of(items, user, timestamp, label, type);
    }

    public List<Item> getItems() {
        return items;
    }
}


