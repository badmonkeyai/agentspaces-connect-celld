/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * The Java connector for a celld or Workers deployment its team cannot
 * change (srad/agentspaces-js.md JS-C-09): {@link
 * ai.badmonkey.agentspaces.connect.celld.CelldManifest} names the base URL,
 * the credential, and the allowlisted routes; {@link
 * ai.badmonkey.agentspaces.connect.celld.CelldAssetProvider} runs the
 * appendix F route-group mapping on {@code agentspaces-connect-core} through
 * {@link ai.badmonkey.agentspaces.connect.celld.CelldClient}; {@link
 * ai.badmonkey.agentspaces.connect.celld.CelldConnector} joins the fleet
 * from a fleet file and serves the manifest.
 */
package ai.badmonkey.agentspaces.connect.celld;
