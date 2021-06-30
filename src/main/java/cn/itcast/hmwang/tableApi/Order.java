package cn.itcast.hmwang.tableApi;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @program: flink-study
 * @description: demo
 * @author: hemwang
 * @create: 2021-06-10 22:35
 **/

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {
    public Long user;
    public String product;
    public int amount;
}
