/* Copyright (c) 2001-2022, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb.util.preprocessor;

import java.io.File;
import java.io.IOException;

/*
 * $Id$
 */
/**
 * Resolves paths using a parent directory; does not resolve properties.
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @version 2.7.0
 * @since 1.8.1
 */
@SuppressWarnings("ClassWithoutLogger")
public class BasicResolver implements IResolver {

    /**
     * creates a new resolver rooted at the given path or its parent.
     *
     * @param path for resolving other abstract paths.
     * @return a new instance.
     * @throws IllegalArgumentException if the path is not a directory, cannot
     *                                  be created as a directory, does not have
     *                                  a parent and a parent cannot be created
     *                                  as a directory, of if the file exist and
     *                                  is not a file or a directory.
     *
     */
    public static IResolver forPath(final String path) {
        if (path == null) {
            return new BasicResolver(null);
        } else if (path.isEmpty()) {
            return new BasicResolver(new File(""));
        }
        final File file = new File(path);
        if (!file.exists()) {
            if (file.mkdirs()) {
                return new BasicResolver(file);
            } else {
                throw new IllegalArgumentException("could not makedirs for: "
                        + file.getAbsolutePath());
            }
        } else if (file.isDirectory()) {
            return new BasicResolver(file);
        } else if (file.isFile()) {
            final File parentFile = file.getParentFile();
            if (parentFile == null) {
                throw new IllegalArgumentException("path is not a directory"
                        + " and has no parent: " + file.getAbsolutePath());
            } else if (parentFile.isFile()) {
                throw new IllegalArgumentException("parent path is a file: "
                        + parentFile.getAbsolutePath());
            } else if (parentFile.isDirectory()) {
                return new BasicResolver(file);
            } else if (parentFile.exists()) {
                throw new IllegalArgumentException("parent path exists but"
                        + " is not a file or directory: "
                        + file.getAbsolutePath());
            } else if (parentFile.mkdirs()) {
                return new BasicResolver(parentFile);
            } else {
                throw new IllegalArgumentException("could not makedirs for: "
                        + parentFile.getAbsolutePath());
            }
        } else {
            throw new IllegalArgumentException("path exists but is not a file"
                    + " or a directory: " + file.getAbsolutePath());
        }
    }

    private final File parentDir;

    public BasicResolver() {
        this(null);
    }

    public BasicResolver(final File parentDir) {
        this.parentDir = parentDir;
    }

    @Override
    public String resolveProperties(final String expression) {
        return expression;
    }

    @Override
    public File resolveFile(final String path) {
        final File dir = this.parentDir;
        final String actualPath = path == null || path.isEmpty() ? "" : path;
        File file = new File(actualPath);
        if (dir != null && !file.isAbsolute()) {
            try {
                file = new File(dir.getCanonicalFile(), file.getPath());
            } catch (IOException ex) {
                file = new File(dir.getAbsoluteFile(), file.getPath());
            }
        }

        try {
            return file.getCanonicalFile();
        } catch (IOException e) {
            return file.getAbsoluteFile();
        }
    }
}
