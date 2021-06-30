package cn.itcast.hmwang.tableApi;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public  class WC{
    private String word;
    private Long frequency;
}
