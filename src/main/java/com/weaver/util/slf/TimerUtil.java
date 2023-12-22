package com.weaver.util.slf;

import com.weaver.util.slf.interfaces.Callback;

import java.util.Timer;
import java.util.TimerTask;

/**
 * @author slf
 * @date 2023/12/21
 * 定时器工具
 */
public class TimerUtil {
    public static long SECOND = 1000;
    public static long MINUTE = 60 * SECOND;
    public static long HOUR = 60 * MINUTE;
    public static long DAY = 24 * HOUR;
    /**
     * 延迟时间后执行一次， 单位毫秒
     * @param task 任务
     * @param delay 延迟
     */
    public static void setTimeout(Callback task, long delay) {
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                task.run();
            }
        }, delay);
    }

    /**
     * 延迟时间后执行第一次，间隔时间循环执行，单位毫秒
     * @param task  任务
     * @param delay 延迟
     * @param interval 时间间隔
     */
    public static void setInterval(Callback task, long delay, long interval) {
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                task.run();
            }
        }, delay, interval);
    }
}
