package com.maxiu.hudi;

import java.io.Serializable;

public interface SerializableFunction<I, O> extends Serializable {
  O call(I v1) throws Exception;
}

