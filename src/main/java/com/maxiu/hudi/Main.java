package com.maxiu.hudi;


import javafx.concurrent.Task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.System.*;

public class Main {
  public static void main(String[] args) {
    int num = 100000;
    List<Integer> list = new ArrayList<>(num);

    for (int i = 0; i < num; i++) {
      list.add(i);
    }

    List<Integer> list2 = new ArrayList<>(num);
    list.stream().parallel().forEach(x -> list2.add(x));
    List<String> collect = list.stream().parallel().map(x -> x + "#").collect(Collectors.toList());

    out.println(list2.size());
    out.println(collect.size());

  }
}
