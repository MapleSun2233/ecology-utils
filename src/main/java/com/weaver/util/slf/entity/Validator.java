package com.weaver.util.slf.entity;

import lombok.AllArgsConstructor;

import java.util.function.Predicate;

/**
 * @author slf
 * @date 2023/10/8
 */
public class Validator<T> {
    private T data;
    private String msg;
    private Predicate<T> predicate;

    public Validator(T data, String msg, Predicate<T> predicate) {
        this.data = data;
        this.msg = msg;
        this.predicate = predicate;
    }

    public void validate() {
        if (predicate.test(data)) {
            throw new RuntimeException(msg);
        }
    }
}
