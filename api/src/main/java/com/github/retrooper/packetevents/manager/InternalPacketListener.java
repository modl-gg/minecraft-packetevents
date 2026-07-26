/*
 * This file is part of packetevents - https://github.com/retrooper/packetevents
 * Copyright (C) 2022 retrooper and contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.github.retrooper.packetevents.manager;

import com.github.retrooper.packetevents.PacketEvents;
import com.github.retrooper.packetevents.event.PacketListenerAbstract;
import com.github.retrooper.packetevents.event.PacketListenerPriority;
import com.github.retrooper.packetevents.event.PacketReceiveEvent;
import com.github.retrooper.packetevents.event.PacketSendEvent;
import com.github.retrooper.packetevents.manager.server.ServerVersion;
import com.github.retrooper.packetevents.protocol.ConnectionState;
import com.github.retrooper.packetevents.protocol.packettype.PacketType;
import com.github.retrooper.packetevents.protocol.player.ClientVersion;
import com.github.retrooper.packetevents.protocol.player.User;
import com.github.retrooper.packetevents.protocol.player.UserProfile;
import com.github.retrooper.packetevents.util.LogManager;
import com.github.retrooper.packetevents.util.mappings.SynchronizedRegistriesHandler;
import com.github.retrooper.packetevents.wrapper.configuration.server.WrapperConfigServerConfigurationEnd;
import com.github.retrooper.packetevents.wrapper.configuration.server.WrapperConfigServerRegistryData;
import com.github.retrooper.packetevents.wrapper.handshaking.client.WrapperHandshakingClientHandshake;
import com.github.retrooper.packetevents.wrapper.login.server.WrapperLoginServerLoginSuccess;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerJoinGame;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerRespawn;
import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Internal
public class InternalPacketListener extends PacketListenerAbstract {

    private final boolean preVia;

    public InternalPacketListener() {
        this(PacketListenerPriority.LOWEST, false);
    }

    public InternalPacketListener(PacketListenerPriority priority) {
        this(priority, false);
    }

    public InternalPacketListener(PacketListenerPriority priority, boolean preVia) {
        super(priority);
        this.preVia = preVia;
    }

    @Override
    public boolean isPreVia() {
        return preVia;
    }

    /**
     * Whether the pre-Via and post-Via connection states are genuinely separate streams.
     * <p>
     * They only are when {@code preViaInjection} puts a second codec, and a matching {@code preVia}
     * listener, into the pipeline — something only the Spigot, Sponge and Fabric platforms ever do.
     * On the proxies the split is inert but not harmless: their codecs build events from the
     * <i>pre</i>-Via state (they pass {@code autoProtocolTranslation = false} to EventCreationUtil)
     * while EventManager only dispatches to the <i>post</i>-Via listener. Updating just the post-Via
     * state there strands the state the codec actually reads on LOGIN for the whole connection, so
     * every packet after login success is decoded against the wrong protocol state.
     */
    private static boolean hasSeparateViaState() {
        return PacketEvents.getAPI().getSettings().isPreViaInjection();
    }

    private void updateEncoderState(User user, ConnectionState state) {
        if (preVia) {
            user.setPreViaEncoderState(state);
        } else if (hasSeparateViaState()) {
            user.setPostViaEncoderState(state);
        } else {
            user.setEncoderState(state); // updates the pre- and post-Via state together
        }
    }

    private void updateDecoderState(User user, ConnectionState state) {
        if (preVia) {
            user.setPreViaDecoderState(state);
        } else if (hasSeparateViaState()) {
            user.setPostViaDecoderState(state);
        } else {
            user.setDecoderState(state); // updates the pre- and post-Via state together
        }
    }

    @Override
    public void onPacketSend(PacketSendEvent event) {
        User user = event.getUser();
        if (event.getPacketType() == PacketType.Login.Server.LOGIN_SUCCESS) {
            // The post-Via listener owns shared state that must only be updated once per channel:
            // user-profile mirroring, channel→UUID mapping, and (for the legacy non-config-aware
            // path) the encoder transition. Skip these on the pre-Via pass to avoid double-writes.
            if (!preVia) {
                Object channel = event.getChannel();
                //Process outgoing login success packet
                WrapperLoginServerLoginSuccess loginSuccess = new WrapperLoginServerLoginSuccess(event);
                UserProfile profile = loginSuccess.getUserProfile();

                //Update user profile
                user.getProfile().setUUID(profile.getUUID());
                user.getProfile().setName(profile.getName());
                //Texture properties are passed in login success on 1.19
                user.getProfile().setTextureProperties(profile.getTextureProperties());

                //Map username with channel
                synchronized (channel) {
                    PacketEvents.getAPI().getProtocolManager().setChannel(profile.getUUID(), channel);
                }

                if (PacketEvents.getAPI().getLogManager().isDebug()) {
                    PacketEvents.getAPI().getLogManager().debug("Mapped player UUID with their channel " + profile.getUUID() + " " + channel);
                }
            }

            // Switch the user's connection state immediately so subsequent packets encode in the right
            // protocol state. Pre-Via and post-Via may transition into different states when ViaVersion
            // bridges a client whose protocol doesn't have the configuration phase to a server whose
            // protocol does (or vice-versa), so route the write through the helpers above.
            //
            // On a proxy the "server version" is only the newest protocol the proxy speaks, never the
            // protocol this player negotiated, so the configuration phase has to be decided from the
            // client version instead.
            ClientVersion clientVersion = user.getClientVersion();
            boolean configurationPhase = PacketEvents.getAPI().getInjector().isProxy() && clientVersion != null
                    ? clientVersion.isNewerThanOrEquals(ClientVersion.V_1_20_2)
                    : event.getServerVersion().isNewerThanOrEquals(ServerVersion.V_1_20_2);
            if (configurationPhase) {
                updateEncoderState(user, ConnectionState.CONFIGURATION);
            } else {
                updateEncoderState(user, ConnectionState.PLAY);
                updateDecoderState(user, ConnectionState.PLAY);
            }
        }

        // The remaining handlers operate on the post-Via stream only — they read packet bodies in
        // the server's protocol and update post-Via state. The pre-Via listener handles its own
        // configuration transitions below.
        if (preVia) {
            if (event.getPacketType() == PacketType.Play.Server.CONFIGURATION_START) {
                updateEncoderState(user, ConnectionState.CONFIGURATION);
            } else if (event.getPacketType() == PacketType.Configuration.Server.CONFIGURATION_END) {
                updateEncoderState(user, ConnectionState.PLAY);
            }
            return;
        }

        // The server sends dimension information in configuration phase, since 1.20.2
        if (event.getPacketType() == PacketType.Configuration.Server.REGISTRY_DATA) {
            WrapperConfigServerRegistryData packet = new WrapperConfigServerRegistryData(event);

            if (packet.getElements() != null) { // 1.20.2 to 1.20.5
                SynchronizedRegistriesHandler.handleRegistry(user, packet,
                        packet.getRegistryKey(), packet.getElements());
            }
            if (packet.getRegistryData() != null) { // since 1.20.5
                SynchronizedRegistriesHandler.handleLegacyRegistries(user, packet, packet.getRegistryData());
            }
        }

        // The server sends registry info in login packet for 1.16 to 1.20.1
        else if (event.getPacketType() == PacketType.Play.Server.JOIN_GAME) {
            WrapperPlayServerJoinGame joinGame = new WrapperPlayServerJoinGame(event);
            user.setEntityId(joinGame.getEntityId());

            if (joinGame.getDimensionCodec() != null) { // 1.16 to 1.20.1
                SynchronizedRegistriesHandler.handleLegacyRegistries(user, joinGame,
                        joinGame.getDimensionCodec());
                user.finalizeRegistries(joinGame);
            }

            user.setDimensionType(joinGame.getDimensionType());
        }

        // Respawn is used to switch dimensions
        else if (event.getPacketType() == PacketType.Play.Server.RESPAWN) {
            WrapperPlayServerRespawn packet = new WrapperPlayServerRespawn(event);
            user.setDimensionType(packet.getDimensionType());
        } else if (event.getPacketType() == PacketType.Play.Server.CONFIGURATION_START) {
            updateEncoderState(user, ConnectionState.CONFIGURATION);
        } else if (event.getPacketType() == PacketType.Configuration.Server.CONFIGURATION_END) {
            updateEncoderState(user, ConnectionState.PLAY);
            user.finalizeRegistries(new WrapperConfigServerConfigurationEnd(event));
        }
    }

    @Override
    public void onPacketReceive(PacketReceiveEvent event) {
        User user = event.getUser();
        if (event.getPacketType() == PacketType.Handshaking.Client.HANDSHAKE) {
            // Handshake arrives once per channel before any Via translation could exist; the post-Via
            // listener is responsible for parsing the wrapper and seeding both directions.
            if (preVia) return;
            WrapperHandshakingClientHandshake packet = new WrapperHandshakingClientHandshake(event);
            ClientVersion clientVersion = packet.getClientVersion();
            ConnectionState state = packet.getNextConnectionState();

            LogManager logger = PacketEvents.getAPI().getLogManager();
            if (logger.isDebug()) {
                logger.debug("Processed handshake for " + event.getAddress() + ": "
                        + state.name() + " / " + packet.getClientVersion().getReleaseName());
            }

            user.setClientVersion(clientVersion);
            user.setConnectionState(state);
        } else if (event.getPacketType() == PacketType.Login.Client.LOGIN_SUCCESS_ACK) {
            updateDecoderState(user, ConnectionState.CONFIGURATION);
        } else if (event.getPacketType() == PacketType.Play.Client.CONFIGURATION_ACK) {
            updateDecoderState(user, ConnectionState.CONFIGURATION);
        } else if (event.getPacketType() == PacketType.Configuration.Client.CONFIGURATION_END_ACK) {
            updateDecoderState(user, ConnectionState.PLAY);
        }
    }
}
