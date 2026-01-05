package org.apache.zookeeper;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.StatPersisted;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class DataNode implements Record {
    // the digest value of this node, calculated from path, data and stat
    private volatile long digest;
    //用于判断digest是否过期 优化性能
    volatile boolean digestCached;

    byte[] data;

    long acl;

    private StatPersisted stat;

    private Set<String> children = null;

    private static final Set<String> EMPTY_SET = Collections.emptySet();

    DataNode() {

    }

    /**
     * @param data
     * @param acl
     * @param stat
     */
    public DataNode(byte[] data, long acl, StatPersisted stat) {
        this.data = data;
        this.acl = acl;
        this.stat = stat;
    }

    public synchronized boolean addChild(String child) {
        if (children == null) {
            children = new HashSet<>(8);
        }
        return children.add(child);
    }

    public synchronized boolean removeChild(String child) {
        if (children == null) {
            children = new HashSet<>(8);
        }
        return children.remove(child);
    }

    public synchronized void setChildren(Set<String> children) {
        this.children = children;
    }


    public synchronized Set<String> getChildren() {
        if (children == null) {
            return EMPTY_SET;
        }
        return Collections.unmodifiableSet(children);
    }

    public synchronized void copyStat(Stat to) {
        to.setAversion(stat.getAversion());
        to.setCtime(stat.getCtime());
        to.setCzxid(stat.getCzxid());
        to.setMtime(stat.getMtime());
        to.setMzxid(stat.getMzxid());
        to.setPzxid(stat.getPzxid());
        to.setVersion(stat.getVersion());
        //to.setEphemeralOwner(getClientEphemeralOwner(stat));
        to.setDataLength(data == null ? 0 : data.length);
        int numChildren = 0;
        if (this.children != null) {
            numChildren = children.size();
        }
        // when we do the Cversion we need to translate from the count of the creates
        // to the count of the changes (v3 semantics)
        // for every create there is a delete except for the children still present
        to.setCversion(stat.getCversion() * 2 - numChildren);
        to.setNumChildren(numChildren);
    }

    @Override
    public void serialize(OutputArchive archive, String tag) throws IOException {
        archive.startRecord(this, "node");
        archive.writeBuffer(data, "data");
        archive.writeLong(acl, "acl");
        stat.serialize(archive, "statpersisted");
        archive.endRecord(this, "node");
    }

    @Override
    public void deserialize(InputArchive archive, String tag) throws IOException {
        archive.startRecord("node");
        data = archive.readBuffer("data");
        acl = archive.readLong("acl");
        stat = new StatPersisted();
        stat.deserialize(archive, "statpersisted");
        archive.endRecord("node");
    }

    public boolean isDigestCached() {
        return digestCached;
    }

    public void setDigestCached(boolean digestCached) {
        this.digestCached = digestCached;
    }

    public long getDigest() {
        return digest;
    }

    public void setDigest(long digest) {
        this.digest = digest;
    }

    public synchronized byte[] getData() {
        return data;
    }
}
