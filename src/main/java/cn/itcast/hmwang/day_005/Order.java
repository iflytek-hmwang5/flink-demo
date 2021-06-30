package cn.itcast.hmwang.day_005;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @program: flink-study
 * @description: 订单类
 * @author: hemwang
 * @create: 2021-05-30 17:52
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order {
    private String orderId;
    private Integer userId;
    private Integer money;
    private Long eventTime;
}
