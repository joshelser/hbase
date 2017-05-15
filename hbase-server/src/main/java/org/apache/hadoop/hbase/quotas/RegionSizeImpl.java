/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.util.ClassSize;

/**
 * An object encapsulating a Region's size and whether it's been reported to the master since
 * the value last changed.
 */
public class RegionSizeImpl implements RegionSize {
  private static final long HEAP_SIZE = ClassSize.OBJECT + ClassSize.ATOMIC_LONG + ClassSize.REFERENCE;
  private final AtomicLong size;

  public RegionSizeImpl(long initialSize) {
    this.size = new AtomicLong(initialSize);
  }

  @Override
  public long heapSize() {
    return HEAP_SIZE;
  }

  @Override
  public RegionSizeImpl setSize(long newSize) {
    // Set the new size before advertising that we need to tell the master about it. Worst case
    // we have to wait for the next period to report it.
    size.set(newSize);
    return this;
  }

  @Override
  public RegionSizeImpl incrementSize(long delta) {
    size.addAndGet(delta);
    return this;
  }

  @Override
  public long getSize() {
    return size.get();
  }
}
