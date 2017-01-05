/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.importer;

import org.voltdb.importer.ChannelChangeCallback;

import java.net.URI;
import java.util.Set;

/**
 * Public api for a ChannelDistributer
 */
public interface ChannelDistributer {

    /**
     * @return a string tag that summarizes the zk versions of opmode and catalog
     */
    String getClusterTag();

    /**
     * Registers a (@link ChannelChangeCallback} for the given importer.
     * @param importer
     * @param callback a (@link ChannelChangeCallback}
     */
    void registerCallback(String importer, ChannelChangeCallback callback);

    /**
     * Register channels for the given importer. If they match to what is already registered
     * then nothing is done. Before registering channels, you need to register a callback
     * handler for channel assignments {@link #registerCallback(String, Object)}
     *
     * @param importer importer designation
     * @param uris list of channel URIs
     */
    void registerChannels(String importer, Set<URI> uris);

    /**
     * Sets the done flag, shuts down its executor thread, and deletes its own host
     * and candidate nodes
     */
    void shutdown();

    /**
     * Unregisters the callback assigned to given importer. Once it is
     * unregistered it can no longer be re-registered
     *
     * @param importer
     */
    void unregisterCallback(String importer);
}
