package cn.itcast.hmwang.day_004;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @program: flink-study
 * @description:
 * @author: hemwang
 * @create: 2021-05-25 23:37
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CarInfo {
    private String sensorId;//信号灯id
    private Integer count;//通过该信号灯的车的数量
}
