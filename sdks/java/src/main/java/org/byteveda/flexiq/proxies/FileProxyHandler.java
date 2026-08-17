package org.byteveda.flexiq.proxies;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.byteveda.flexiq.errors.ProxyException;

/**
 * Proxies a {@link File} by its absolute path. An optional allowlist of root
 * directories is enforced on reconstruct, so a tampered or hostile ref cannot
 * resolve to a path outside them (an empty allowlist permits any path).
 */
public final class FileProxyHandler implements ProxyHandler<File> {
    private final List<Path> allowedRoots;

    public FileProxyHandler() {
        this(List.of());
    }

    public FileProxyHandler(List<Path> allowedRoots) {
        List<Path> roots = new ArrayList<>(allowedRoots.size());
        for (Path root : allowedRoots) {
            // Resolve each root to its real path so containment is checked against
            // the true filesystem location, not a symlinked alias.
            roots.add(realPath(root.toAbsolutePath().normalize()));
        }
        this.allowedRoots = List.copyOf(roots);
    }

    @Override
    public String id() {
        return "file";
    }

    @Override
    public boolean handles(Object value) {
        return value instanceof File;
    }

    @Override
    public Map<String, Object> deconstruct(File value) {
        return Map.of("path", value.getAbsolutePath());
    }

    @Override
    public File reconstruct(Map<String, Object> reference) {
        if (reference == null) {
            throw new ProxyException("file proxy ref has no reference");
        }
        Object path = reference.get("path");
        if (!(path instanceof String)) {
            throw new ProxyException("file proxy ref missing 'path'");
        }
        Path resolved = Paths.get((String) path).toAbsolutePath().normalize();
        if (!allowed(resolved)) {
            throw new ProxyException("file path not in allowlist: " + resolved);
        }
        return resolved.toFile();
    }

    private boolean allowed(Path path) {
        if (allowedRoots.isEmpty()) {
            return true;
        }
        Path real = realPath(path);
        for (Path root : allowedRoots) {
            if (real.startsWith(root)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The real (symlink-resolved) path. Since the target may not exist yet, resolve
     * the nearest existing ancestor to its real path — collapsing any symlinked
     * ancestor — then re-append the remaining segments. A path whose ancestors do
     * not exist yet has no symlink to hide behind, so its normalized form stands.
     */
    private static Path realPath(Path path) {
        Path existing = path;
        while (existing != null && !Files.exists(existing)) {
            existing = existing.getParent();
        }
        if (existing == null) {
            return path;
        }
        try {
            Path realExisting = existing.toRealPath();
            return realExisting.resolve(existing.relativize(path)).normalize();
        } catch (IOException e) {
            throw new ProxyException("failed to resolve real path for " + path, e);
        }
    }
}
