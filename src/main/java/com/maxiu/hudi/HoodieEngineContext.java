package com.maxiu.hudi;

import java.util.List;

public abstract class HoodieEngineContext {
  public abstract  <I, O> List<O> map(List<I> data, SerializableFunction<I, O> func, int parallelism) ;
}
