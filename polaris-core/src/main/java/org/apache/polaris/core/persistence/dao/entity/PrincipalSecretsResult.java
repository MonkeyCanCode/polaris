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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;

/** the result of load/rotate principal secrets */
public class PrincipalSecretsResult extends BaseResult {

  // principal client identifier and associated secrets. Null if error
  private final PolarisPrincipalSecrets principalSecrets;

  /**
   * Constructor for an error
   *
   * @param errorCode error code, cannot be SUCCESS
   * @param extraInformation extra information
   */
  public PrincipalSecretsResult(
      @Nonnull ReturnStatus errorCode, @Nullable String extraInformation) {
    super(errorCode, extraInformation);
    this.principalSecrets = null;
  }

  /**
   * Constructor for success
   *
   * @param principalSecrets and associated secret information
   */
  public PrincipalSecretsResult(@Nonnull PolarisPrincipalSecrets principalSecrets) {
    super(ReturnStatus.SUCCESS);
    this.principalSecrets = principalSecrets;
  }

  @JsonCreator
  private PrincipalSecretsResult(
      @JsonProperty("returnStatus") @Nonnull ReturnStatus returnStatus,
      @JsonProperty("extraInformation") @Nullable String extraInformation,
      @JsonProperty("principalSecrets") @Nonnull PolarisPrincipalSecrets principalSecrets) {
    super(returnStatus, extraInformation);
    this.principalSecrets = principalSecrets;
  }

  public PolarisPrincipalSecrets getPrincipalSecrets() {
    return principalSecrets;
  }
}
