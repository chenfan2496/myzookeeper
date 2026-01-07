package org.apache.zookeeper.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

public class PathTrie {
    private static Logger LOG = LoggerFactory.getLogger(PathTrie.class);
    /**
     * Root node of PathTrie
     */
    private final TrieNode rootNode;


    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

    private final Lock readLock = lock.readLock();

    private final Lock writeLock = lock.writeLock();

    static class TrieNode {
        final String value;
        final Map<String, TrieNode> children;
        boolean property;
        TrieNode parent;

        /**
         * Create a trie node with parent as parameter.
         *
         * @param parent the parent of this node
         * @param value  the value stored in this node
         */
        private TrieNode(TrieNode parent, String value) {
            this.value = value;
            this.parent = parent;
            this.property = false;
            this.children = new HashMap<>(4);
        }

        TrieNode getParent() {
            return this.parent;
        }

        /**
         * set the parent of this node.
         *
         * @param parent the parent to set to
         */
        void setParent(TrieNode parent) {
            this.parent = parent;
        }

        /**
         * A property that is set for a node - making it special.
         */
        void setProperty(boolean prop) {
            this.property = prop;
        }

        /**
         * The property of this node.
         *
         * @return the property for this node
         */
        boolean hasProperty() {
            return this.property;
        }

        /**
         * The value stored in this node.
         *
         * @return the value stored in this node
         */
        public String getValue() {
            return this.value;
        }

        void addChildren(String childName, TrieNode node) {
            this.children.put(childName, node);
        }

        void deleteChild(String childName) {
            this.children.computeIfPresent(childName, (key, childNode) -> {
                childNode.setProperty(false);
                if (childNode.isLeafNode()) {
                    return null;
                }
                return childNode;
            });
        }

        /**
         * Return the child of a node mapping to the input child name.
         *
         * @param childName the name of the child
         * @return the child of a node
         */
        TrieNode getChild(String childName) {
            return this.children.get(childName);
        }

        /**
         * Get the list of children of this trienode.
         *
         * @return A collection containing the node's children
         */
        Collection<String> getChildren() {
            return children.keySet();
        }

        /**
         * Determine if this node is a leaf (has no children).
         *
         * @return true if this node is a lead node; otherwise false
         */
        boolean isLeafNode() {
            return children.isEmpty();
        }

        @Override
        public String toString() {
            return "TrieNode [name=" + value + ", property=" + property + ", children=" + children.keySet() + "]";
        }
    }


    public PathTrie() {
        this.rootNode = new TrieNode(null, "/");
    }

    public void addPath(final String path) {
        Objects.requireNonNull(path, "Path cannot be null");
        if (path.length() == 0) {
            throw new IllegalArgumentException("Invalid path: " + path);
        }
        final String[] pathComponents = split(path);
        writeLock.lock();
        try {
            TrieNode parent = rootNode;
            for (final String component : pathComponents) {
                TrieNode child = parent.children.get(component);
                if (child == null) {
                    child = new TrieNode(parent, component);
                    parent.addChildren(component, child);
                }
                parent = child;
            }
            parent.setProperty(true);
        } finally {
            writeLock.unlock();
        }
    }

    public void deletePath(final String path) {
        Objects.requireNonNull(path, "Path cannot be null");
        if (path.length() == 0) {
            throw new IllegalArgumentException("Invalid path: " + path);
        }
        final String[] pathComponents = split(path);
        writeLock.lock();
        try {
            TrieNode parent = rootNode;
            for (final String component : pathComponents) {
                TrieNode child = parent.children.get(component);
                if (child == null) {
                    return;
                }
                parent = child;
            }
            LOG.debug("{}", parent);
            final TrieNode realParent = parent.getParent();
            realParent.deleteChild(parent.getValue());
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Return true if the given path exists in the trie, otherwise return false;
     * All paths are relative to the root node.
     *
     * @param path the input path
     * @return the largest prefix for the
     */
    public boolean existsNode(final String path) {
        Objects.requireNonNull(path, "Path cannot be null");

        if (path.length() == 0) {
            throw new IllegalArgumentException("Invalid path: " + path);
        }
        final String[] pathComponents = split(path);

        readLock.lock();
        try {
            TrieNode parent = rootNode;
            for (final String part : pathComponents) {
                if (parent.getChild(part) == null) {
                    // the path does not exist
                    return false;
                }
                parent = parent.getChild(part);
                LOG.debug("{}", parent);
            }
        } finally {
            readLock.unlock();
        }
        return true;
    }

    public String findMaxPrefix(String path) {
        Objects.requireNonNull(path, "Path cannot be null");

        if (path.length() == 0) {
            throw new IllegalArgumentException("Invalid path: " + path);
        }
        final String[] pathComponents = split(path);

        readLock.lock();
        try {
            TrieNode parent = rootNode;
            TrieNode deepestPropertyNode = null;
            for (String component : pathComponents) {
                parent = parent.children.get(component);
                if( parent == null ) {
                    LOG.debug("{}", component);
                    break;
                }
                if(parent.hasProperty()) {
                    deepestPropertyNode = parent;
                }
            }

            if (deepestPropertyNode == null) {
                return "/";
            }
            final Deque<String> treePath = new ArrayDeque<>();
            TrieNode node = deepestPropertyNode;
            while(node != rootNode) {
                treePath.offerFirst(node.value);
                node = node.parent;
            }
            return "/" + String.join("/",treePath);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Clear all nodes in the trie.
     */
    public void clear() {
        writeLock.lock();
        try {
            rootNode.getChildren().clear();
        } finally {
            writeLock.unlock();
        }
    }


    private static String[] split(final String path) {
        return Stream.of(path.split("/")).filter(t -> !t.trim().isEmpty()).toArray(String[]::new);
    }
}
