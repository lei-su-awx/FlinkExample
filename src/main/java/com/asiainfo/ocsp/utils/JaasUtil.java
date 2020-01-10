package com.asiainfo.ocsp.utils;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

/**
 * @author sulei
 * @date 2020/1/9
 * @e-mail 776531804@qq.com
 *
 * 解析jaas文件
 *
 */

public class JaasUtil {
    /**
     * 解析并返回配置项
     *
     * @param path        jaas文件路径
     * @param contextName jaas文件中需要解析的context name，例如Client, KafkaClient
     * @return
     */
    public static String getProperties(String path, String contextName) throws Exception {
        String props = "";

        File jaasFile = new File(path);
        if (!jaasFile.exists() || jaasFile.isDirectory()) {
            throw new FileNotFoundException("jaas file : " + path + " do not exists!");
        }

        List<String> jaasLines = FileUtils.readLines(jaasFile, "UTF-8");
        for (int i = 0; i < jaasLines.size(); i++) {
            String line = jaasLines.get(i);
            if (line.trim().startsWith(contextName)) {
                props = getContextProps(jaasLines, ++i);
                break;
            }
        }

        return props;
    }

    public static String getContextProps(List<String> lines, int i) {
        StringBuilder props = new StringBuilder();
        for (; i < lines.size(); i++) {
            String line = lines.get(i);
            if (!line.trim().startsWith("};")) {
                props.append(line).append(" ");
            } else {
                break;
            }
        }
        return props.toString();
    }

    public static void main(String[] args) throws Exception {
        String jaasPath = "/path/to/jaas.conf";
        System.out.println(getProperties(jaasPath, "KafkaClient"));
    }

}
