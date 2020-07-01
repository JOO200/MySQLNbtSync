package de.terraconia.nbtsync;

import de.baba43.lib.plugin.BabaJavaPlugin;
import de.baba43.serverapi.plugin.ServerAPI;

public class NbtSyncPlugin extends BabaJavaPlugin {

    @Override
    public void onEnable() {
        super.onEnable();
        new NbtDatabase(this, ServerAPI.getAPI().getReporter(this), ServerAPI.getAPI().getMainDatabase());
    }
}
