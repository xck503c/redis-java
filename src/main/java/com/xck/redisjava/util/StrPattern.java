package com.xck.redisjava.util;

import cn.hutool.core.util.StrUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * 字符串匹配工具
 *
 * @author xuchengkun
 * @date 2021/09/23 18:20
 **/
public class StrPattern {

    boolean isLockHead = false;
    boolean isLockTail = false;
    List<String> keywords;

    public boolean isMatch(String str) {
        int start = 0, end = keywords.size();
        if (isLockHead && str.startsWith(keywords.get(0))) {
            ++start;
        }

        if (isLockTail && str.endsWith(keywords.get(keywords.size() - 1))) {
            --end;
        }

        int lastIndex = 0;
        for (int i = start; i < end; i++) {
            lastIndex = str.indexOf(keywords.get(i), 0);
            if (lastIndex == -1) {
                return false;
            }
        }
        return true;
    }

    public static StrPattern build(String args) {
        StrPattern strPattern = new StrPattern();

        if (!args.startsWith("*")) {
            strPattern.setLockHead(true);
        }
        if (!args.endsWith("*")) {
            strPattern.setLockTail(true);
        }

        List<String> keyList = new ArrayList<>();
        boolean isLastAsterisk = false;
        String[] tmp = args.split("\\*");
        for (int i = 0; i < tmp.length; i++) {
            if (StrUtil.isBlank(tmp[i])) {
                continue;
            }
            String s = tmp[i].trim();
            if (!isLastAsterisk && s.equals("*")) {
                isLastAsterisk = true;
                keyList.add(s);
            } else if (isLastAsterisk && s.equals("*")) {
                continue;
            } else {
                keyList.add(s);
            }
        }
        strPattern.setKeywords(keyList);
        return strPattern;
    }

    public boolean isLockHead() {
        return isLockHead;
    }

    public void setLockHead(boolean lockHead) {
        isLockHead = lockHead;
    }

    public boolean isLockTail() {
        return isLockTail;
    }

    public void setLockTail(boolean lockTail) {
        isLockTail = lockTail;
    }

    public List<String> getKeywords() {
        return keywords;
    }

    public void setKeywords(List<String> keywords) {
        this.keywords = keywords;
    }
}
