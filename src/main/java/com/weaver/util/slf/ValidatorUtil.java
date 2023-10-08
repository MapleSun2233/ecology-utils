package com.weaver.util.slf;

import com.weaver.util.slf.entity.Validator;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * @author slf
 * @date 2023/10/8
 */
public class ValidatorUtil {
    private final List<Validator> validatorList;
    private ValidatorUtil() {
        validatorList = new ArrayList<>();
    }
    public static ValidatorUtil builder() {
        return new ValidatorUtil();
    }
    public <T> ValidatorUtil append(T data, Predicate<T> predicate, String msg) {
        validatorList.add(new Validator<>(data, msg, predicate));
        return this;
    }
    public void validate() throws RuntimeException{
        validatorList.forEach(Validator::validate);
    }
    public static <T> void validate(T data, Predicate<T> predicate, String msg) throws RuntimeException{
        ValidatorUtil.builder().append(data, predicate, msg).validate();
    }
}
