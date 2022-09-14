package org.hsqldb.testbase;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Objects;
import java.util.jar.JarInputStream;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ResourceCollector {

    private final Pattern nameMatcher;

    public ResourceCollector(Pattern nameMatcher) {
        this.nameMatcher = Objects.requireNonNull(nameMatcher, "nameMatcher must not be null");
    }

    public void collectResources(final URL location, final Collection<String> resources) throws IOException, URISyntaxException {

        switch (location.getProtocol()) {
            case "file": {
                final File file = Paths.get(location.toURI()).toFile();
                if (file.isDirectory()) {
                    collectDirectoryResources(file, resources);
                }
                break;
            }
            case "jar": {
                collectJarResources(location, resources);
                break;
            }

        }
    }

    private void collectJarResources(final URL location, final Collection<String> resources) throws IOException {

        try (final ZipInputStream zis = new JarInputStream(location.openStream(), true);) {
            while (zis.available() == 1) {
                final ZipEntry ze = zis.getNextEntry();
                final String fileName = ze.getName();
                final boolean accept = nameMatcher.matcher(fileName).matches();
                if (accept) {
                    resources.add(fileName);
                }
                zis.closeEntry();
            }
        }
    }

    private void collectDirectoryResources(final File directory, Collection<String> resources) {
        final File[] fileList = directory.listFiles();
        for (final File file : fileList) {
            if (file.isDirectory()) {
                collectDirectoryResources(file, resources);
            } else {
                try {
                    final String fileName = file.getCanonicalPath();
                    final boolean accept = nameMatcher.matcher(fileName).matches();
                    if (accept) {
                        resources.add(fileName);
                    }
                } catch (final IOException e) {
                    throw new Error(e);
                }
            }
        }
    }
}
