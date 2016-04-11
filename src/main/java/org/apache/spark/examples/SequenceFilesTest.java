/**
 * Copyright (C) 2015 Baifendian Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples;

/**
 * <p>
 * 
 * @author : dsfan
 *
 * @date : 2016年3月22日
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;

public class SequenceFilesTest {
    private static final String[] DATA = { "One", "Two", "Three", "aaa", "bbb", "ccc" };

    public static void main(String[] args) throws IOException {
        String hdfsUri = "hdfs://hlg-2p238-fandongsheng:8020";
        String pathStr = "/tmp/example/seq1";
        String compressType = "1";

        // 下面配置供windows操作系统下使用
        // System.setProperty("hadoop.home.dir", "E:\\tools");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        Path path = new Path(pathStr);

        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;
        try {
            SequenceFile.Writer.Option pathOpt = SequenceFile.Writer.file(path);
            SequenceFile.Writer.Option keyClassOpt = SequenceFile.Writer.keyClass(key.getClass());
            SequenceFile.Writer.Option valueClassOpt = SequenceFile.Writer.valueClass(value.getClass());
            SequenceFile.Writer.Option compressionOpt = null;

            // compress type
            if (compressType.equals("1")) {
                System.out.println("compress none");
                compressionOpt = SequenceFile.Writer.compression(CompressionType.NONE);
            } else if (compressType.equals("2")) {
                System.out.println("compress record");
                compressionOpt = SequenceFile.Writer.compression(CompressionType.RECORD);
            } else if (compressType.equals("3")) {
                System.out.println("compress block");
                compressionOpt = SequenceFile.Writer.compression(CompressionType.BLOCK);
            } else {
                System.out.println("Default : compress none");
                compressionOpt = SequenceFile.Writer.compression(CompressionType.NONE);
            }

            writer = SequenceFile.createWriter(conf, pathOpt, keyClassOpt, valueClassOpt, compressionOpt);

            for (int i = 0; i < 100; i++) {
                key.set(100 - i);
                value.set(DATA[i % DATA.length]);
                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
                writer.append(key, value);

            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }
}
