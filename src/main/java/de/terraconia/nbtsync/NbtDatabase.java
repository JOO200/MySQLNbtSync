package de.terraconia.nbtsync;

import de.baba43.lib.async.db.SQLController;
import de.baba43.lib.async.db.v2.FutureDbController;
import de.baba43.lib.log.IReporter;
import de.baba43.lib.plugin.BabaJavaPlugin;
import de.baba43.serverapi.plugin.ServerAPI;
import de.terraconia.terrapaper.NBTStorage;
import de.terraconia.terrapaper.NBTStorageProvider;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.event.Listener;

import java.io.ByteArrayInputStream;
import java.sql.*;
import java.util.*;

public class NbtDatabase extends FutureDbController implements NBTStorage, Listener {
    private static final List<String> saveTags = List.of(
            "bukkit", "SleepTimer", "Attributes", "Invulnerable", "AbsorptionAmount", "abilities",
            "FallDistance", "recipeBook", "DeathTime", "XpSeed", "Spigot.ticksLived", "XpTotal",
            "playerGameType", "seenCredits", "Health", "foodSaturationLevel", "Air",
            "XpLevel", "Score", "Fire", "XpP", "EnderItems", "Paper", "DataVersion",
            "foodLevel", "foodExhaustionLevel", "HurtTime", "SelectedItemSlot", "Inventory", "foodTickTimer"
    );

    @Override
    public Collection<String> getKeys() {
        return saveTags;
    }

    private String table = "survival_global.nbtData";
    private BabaJavaPlugin plugin;
    Map<UUID, Long> pendingInventories = new HashMap<>();

    public NbtDatabase(BabaJavaPlugin plugin, IReporter log, SQLController db) {
        super(plugin, log, db);
        this.plugin = plugin;
        NBTStorageProvider.setNBTStorage(this);
        Bukkit.getPluginManager().registerEvents(this, plugin);
        genTable();
        startRunnable();
    }

    private void startRunnable() {
        Bukkit.getScheduler().runTaskTimer(plugin, () -> {
            pendingInventories.entrySet().removeIf(uuidLongEntry -> loadOrOld(uuidLongEntry.getKey(), uuidLongEntry.getValue()));
        }, 10, 10);
    }

    private boolean loadOrOld(UUID uuid, long timestamp) {
        Player player = Bukkit.getPlayer(uuid);
        long currentTime = System.currentTimeMillis();
/*
        if(player == null && currentTime - timestamp > 1000) {
            plugin.getLogger().info("Unknown player with uuid " + uuid + " found.");
            return true;
        }
  */
        Bukkit.getLogger().info("Checkin uuid " + uuid);
        if(currentTime - timestamp > 10000) {
            if (player != null) player.kickPlayer("Dein Inventar wurde nicht korrekt geladen. Bitte melde dich im Forum beim Team.");
            return true;
        }

        if(releaseLockOpen(uuid)) {
            if (player != null) {
                player.loadData();
                player.setWaitingForInventory(false);
            }
            return true;
        }
        return false;
    }
/*
    @EventHandler
    public void onPreLogin(AsyncPlayerPreLoginEvent event) {
        try {
            loadNBTTagCompound(event.getUniqueId());
        } catch(NBTStorageNotReady storageNotReady) {
            plugin.getLogger().info("Unable to load user from storage because the lock is not released.");
        }
    }*/

    @Override
    public byte[] load(UUID uuid) throws NBTStorageNotReady {
        try {
            Bukkit.getLogger().info("Loading NBT tags from database");
            return loadNBTTagCompound(uuid);
        } catch (NBTStorageNotReady notReady) {
            pendingInventories.put(uuid, System.currentTimeMillis());
            throw notReady;
        }
    }

    private byte[] loadNBTTagCompound(UUID uniqueId) throws NBTStorageNotReady {
        byte[] data = null;
        String latestServer = "";
        try (Connection connection = getConnection()) {
            PreparedStatement stmt = connection.prepareStatement("SELECT * FROM " + table + " WHERE uuid = ?");
            stmt.setString(1, uniqueId.toString());
            ResultSet rs = stmt.executeQuery();
            if(!rs.next()) {
                Bukkit.getLogger().info("new Player found");
                rs.close();
                stmt.close();
                return null;
            }

            if ((latestServer = rs.getString("current")) == null || latestServer.equals(ServerAPI.getServerName())) {
                Bukkit.getLogger().info("Latest server connected: " + latestServer);
                data = rs.getBytes("data");

                stmt = connection.prepareStatement("UPDATE " + table + " SET current = ? WHERE uuid=?");
                stmt.setString(1, ServerAPI.getServerName());
                stmt.setString(2, uniqueId.toString());
                stmt.executeUpdate();
                stmt.close();
            } else {
                rs.close();
                stmt.close();
                Bukkit.getLogger().info("Exception");
                throw new NBTStorageNotReady();
            }
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return data;

    }

    private boolean releaseLockOpen(UUID uuid) {
        try (Connection connection = getConnection()) {
            PreparedStatement stmt = connection.prepareStatement("SELECT * FROM " + table + " WHERE uuid = ?");
            stmt.setString(1, uuid.toString());
            ResultSet rs = stmt.executeQuery();
            if(!rs.next()) {
                Bukkit.getLogger().info("new Player found");
                rs.close();
                stmt.close();
                return false;
            }
            String latestServer;
            boolean value =  ((latestServer = rs.getString("current")) == null || latestServer.equals(ServerAPI.getServerName()));
            rs.close();
            stmt.close();
            return value;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public void save(UUID uuid, byte[] bytes, boolean releaseLock) {
        try (Connection connection = getConnection()) {
            String sql = "INSERT INTO " + table + " (uuid, current, data) VALUES (?,?,?)" +
                    "ON DUPLICATE KEY UPDATE data = ?";
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setString(1, uuid.toString());
            stmt.setString(2, ServerAPI.getServerName());
            stmt.setBlob(3, new ByteArrayInputStream(bytes));
            stmt.setBlob(4, new ByteArrayInputStream(bytes));
            stmt.executeUpdate();
            stmt.close();

            stmt = connection.prepareStatement("UPDATE " + table + " SET current = ? WHERE uuid=?");
            stmt.setString(1, releaseLock ? null : ServerAPI.getServerName());
            stmt.setString(2, uuid.toString());
            stmt.executeUpdate();
            stmt.close();

            Bukkit.getLogger().info("Finished save: releasedLock=" + String.valueOf(releaseLock));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }



    private void genTable() {
        try(Connection connection = getConnection()) {
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS " + table + " (" +
                            "`uuid` VARCHAR(36) NOT NULL, " +
                            "`current` VARCHAR(36) NOT NULL, " +
                            "`lastSaved` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP , " +
                            "`data` LONGTEXT NOT NULL," +
                            "PRIMARY KEY (uuid));");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
