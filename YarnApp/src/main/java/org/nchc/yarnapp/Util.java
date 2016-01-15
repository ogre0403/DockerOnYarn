package org.nchc.yarnapp;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;

/**
 * Created by superorange on 10/20/15.
 */
public class Util {

    static LocalResource newYarnAppResource(FileSystem fs, Path path)
            throws IOException {
        return Util.newYarnAppResource(fs, path, LocalResourceType.FILE,
                LocalResourceVisibility.APPLICATION);
    }
    static LocalResource newYarnAppResource(FileSystem fs, Path path,
                                            LocalResourceType type, LocalResourceVisibility vis) throws IOException {
        Path qualified = fs.makeQualified(path);
        FileStatus status = fs.getFileStatus(qualified);
        LocalResource resource = Records.newRecord(LocalResource.class);
        resource.setType(type);
        resource.setVisibility(vis);
        resource.setResource(ConverterUtils.getYarnUrlFromPath(qualified));
        resource.setTimestamp(status.getModificationTime());
        resource.setSize(status.getLen());
        return resource;
    }
}
