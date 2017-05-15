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

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * A {@link RegionSizeStore} implementation backed by a ConcurrentHashMap. We expected similar
 * amounts of reads and writes to the "store", so using a RWLock is not going to provide any
 * exceptional gains.
 */
public class RegionSizeStoreImpl implements RegionSizeStore {
  private static final Log LOG = LogFactory.getLog(RegionSizeStoreImpl.class);
  private final ConcurrentHashMap<HRegionInfo,RegionSize> store;
  
  public RegionSizeStoreImpl() {
    store = new ConcurrentHashMap<>();
  }

  @Override
  public Iterator<Entry<HRegionInfo,RegionSize>> iterator() {
    return store.entrySet().iterator();
  }

  @Override
  public RegionSize getRegionSize(HRegionInfo regionInfo) {
    return store.get(regionInfo);
  }

  @Override
  public void put(HRegionInfo regionInfo, long size) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Setting space quota size for " + regionInfo + " to " + size);
    }
    // Atomic. Either sets the new size for the first time, or replaces the existing value.
    store.compute(regionInfo,
        (key,value) -> value == null ? new RegionSizeImpl(size) : value.setSize(size)); 
  }

  @Override
  public void incrementRegionSize(HRegionInfo regionInfo, long delta) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Updating space quota size for " + regionInfo + " with a delta of " + delta);
    }
    // Atomic. Recomputes the stored value with the delta if there is one, otherwise use the delta.
    store.compute(regionInfo,
        (key,value) -> value == null ? new RegionSizeImpl(delta) : value.incrementSize(delta)); 
  }

  @Override
  public RegionSize remove(HRegionInfo regionInfo) {
    return store.remove(regionInfo);
  }

  @Override
  public long heapSize() {
    final long sizeOfEntry = ClassSize.align(
        ClassSize.CONCURRENT_HASHMAP_ENTRY
        + ClassSize.OBJECT + Bytes.SIZEOF_LONG
        // TODO Have HRegionInfo implement HeapSize. 100B is based on a heapdump.
        + ClassSize.OBJECT + 100);
    // Will have to iterate over each element if HRegionInfo implements HeapSize, for now it's just
    // a simple calculation.
    return sizeOfEntry * store.size();
  }

  @Override
  public int size() {
    return store.size();
  }

  @Override
  public boolean isEmpty() {
    return store.isEmpty();
  }

  @Override
  public void clear() {
    store.clear();
  }
}
