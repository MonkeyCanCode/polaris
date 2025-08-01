/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.core.persistence.dao.entity;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.storage.AccessConfig;

/** Result of a getSubscopedCredsForEntity() call */
public class ScopedCredentialsResult extends BaseResult {

  // null if not success. Else, set of name/value pairs for the credentials
  private final AccessConfig accessConfig;

  /**
   * Constructor for an error
   *
   * @param errorCode error code, cannot be SUCCESS
   * @param extraInformation extra information
   */
  public ScopedCredentialsResult(
      @Nonnull ReturnStatus errorCode, @Nullable String extraInformation) {
    super(errorCode, extraInformation);
    this.accessConfig = null;
  }

  /**
   * Constructor for success
   *
   * @param accessConfig credentials
   */
  public ScopedCredentialsResult(AccessConfig accessConfig) {
    super(ReturnStatus.SUCCESS);
    this.accessConfig = accessConfig;
  }

  public AccessConfig getAccessConfig() {
    return accessConfig;
  }
}
