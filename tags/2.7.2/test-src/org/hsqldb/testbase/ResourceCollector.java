package org.hsqldb.testbase;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Objects;
import java.util.jar.JarInputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ResourceCollector {

    private static final Logger LOG = Logger.getLogger(ResourceCollector.class.getName());

    private final Pattern nameMatcher;

    /**
     * Constructs a new instance with the given {@code nameMatcher}.
     *
     * @param nameMatcher used to filter collected resources.
     */
    public ResourceCollector(final Pattern nameMatcher) {
        this.nameMatcher = Objects.requireNonNull(nameMatcher, "nameMatcher must not be null");
    }

    /**
     *
     * @param location  from which to collect resources
     * @param resources collection into which to place resources from the given
     *                  location
     * @throws NullPointerException if location is null or resources is null.
     * @throws IOException          if an I/O error occurs.
     * @throws URISyntaxException   if the location URL is not formatted
     *                              strictly according to RFC2396 and cannot
     *                              be converted to a URI.
     */
    public void collectResources(final URL location, final Collection<String> resources) throws IOException, URISyntaxException {
        Objects.requireNonNull(location, "location URL must not be null.");
        Objects.requireNonNull(resources, "resources collection must not be null.");

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

    private void collectJarResources(final URL location, final Collection<String> resources) {

        try (final ZipInputStream zis = new JarInputStream(location.openStream(), true);) {
            try {
                while (zis.available() == 1) {
                    try {
                        final ZipEntry ze = zis.getNextEntry();
                        final String fileName = ze.getName();
                        final boolean accept = nameMatcher.matcher(fileName).matches();
                        if (accept) {
                            resources.add(fileName);
                        }
                        try {
                            zis.closeEntry();
                        } catch (IOException ex) {
                            LOG.log(Level.FINE, "ziz.closeEntry()", ex);
                        }
                    } catch (IOException ex) {
                        LOG.log(Level.WARNING, "ziz.getNextEntry()", ex);
                    }
                }
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "zis.available()", ex);
            }
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, location.toString(), ex);
        } catch (Throwable t) {
            LOG.log(Level.SEVERE, null, t);
        }
    }

    private void collectDirectoryResources(final File directory, Collection<String> resources) {

        try {
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
                        LOG.log(Level.SEVERE, file.getAbsolutePath(), e);
                    }
                }
            }
        } catch (Throwable t) {
            LOG.log(Level.SEVERE, null, t);
        }
    }
}
