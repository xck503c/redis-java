package com.xck.redisjava.base;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 有序集合
 * <p>
 * Hello. Z is as in XYZ, so the idea is, sets with another dimension: the order. It's a far association... I know :)
 * <p>
 * 简要说明：
 * 1. 有序集合：跳表+字典结构。字典k-成员，v-分值
 * 2. 跳表节点具有span，forward数组，分值，成员value
 * (1) span是跨度，用来快速定位当前节点的位置，也就是rank
 * (2) forward数组是一组指向后面节点的指针，查询的时候从最高开始查，
 * 就像跳跃一样，分层。
 * (3) 成员唯一，分值可以重复
 * 3. 字典结构为的是索引：成员->分值
 *
 * @author xuchengkun
 * @date 2021/09/15 14:44
 **/
public class ZSet {

    private static int MAX_LEVEL = 16;
    private static int level = 1;

    //头尾结点
    private SkipListNode header;
    private SkipListNode tail;
    //链表长度
    private int len;

    private Map<Sds, Integer> member2Score;

    public ZSet() {
        header = new SkipListNode();
        tail = header;
        member2Score = new HashMap<>();
    }

    /**
     * 根据成员寻找节分数值 ZSCORE
     *
     * @param member
     * @return
     */
    public Integer findScoreByMember(Sds member) {
        return member2Score.get(member);
    }

    /**
     * 返回指定排位的节点
     *
     * @param rank
     * @return
     */
    public SkipListNode findElemByRank(int rank) {
        /**
         * 从header开始寻找，叠加span
         */
        int span = 0; //寻找的过程也是跨度累加的过程
        SkipListNode p = header;
        for (int i = level - 1; i >= 0; i--) {
            SkipListLevel skipListLevel = null;
            while ((skipListLevel = p.forwards[i]) != null) {
                if (skipListLevel.getSpan() + span > rank) {
                    break;
                }
                span += skipListLevel.getSpan();
                p = skipListLevel.getForward();
            }
            if (span == rank) {
                return p;
            }
        }
        return null;
    }

    /**
     * 返回指定分数和成员的排位
     *
     * @param member
     * @return -1-找不到
     */
    public int getElemRank(double score, Sds member) {
        /**
         * 从header开始寻找
         */
        int span = 0;
        SkipListNode p = header;
        for (int i = level - 1; i >= 0; i--) {
            SkipListLevel skipListLevel = null;
            while ((skipListLevel = p.forwards[i]) != null) {
                //forward指向的>要寻找的
                SkipListNode next = skipListLevel.getForward();
                if (next.getScore() > score || next.getMember().compareTo(member) == 1) {
                    break;
                } else if (next.getScore() < score || next.getMember().compareTo(member) == -1) {
                    span += skipListLevel.getSpan();
                    p = next;
                } else {
                    span += skipListLevel.getSpan();
                    return span;
                }

            }
        }
        return -1;
    }

    /**
     * 寻找第一个在指定分值范围内的节点
     *
     * @param minScore
     * @param maxScore
     * @return
     */
    public SkipListNode findFirstInScoreRange(int minScore, int maxScore) {

        if (header.forwards[0] == null) {
            return null;
        }
        //如果第一个节点刚刚好在这个区间，那就直接返回
        SkipListNode first = header.forwards[0].getForward();
        if (first.getScore() >= minScore && first.getScore() <= maxScore) {
            return header;
        } else if (first.getScore() > maxScore || tail.getScore() < minScore) {
            return null;
        }

        SkipListNode p = header;
        for (int i = level - 1; i >= 0; i--) {
            SkipListLevel skipListLevel = null;
            while ((skipListLevel = p.forwards[i]) != null) {
                SkipListNode next = skipListLevel.getForward();
                //都比min还小，那就往右继续找，直到发现下一个已经超过最小值
                if (next.getScore() < minScore) {
                    p = next;
                    continue;
                }
                break;
            }
        }

        //最后找到一定最接近minScore的
        p = p.forwards[0].getForward();

        if (p.getScore() >= minScore && p.getScore() <= maxScore) {
            return p;
        }
        return null;
    }

    /**
     * 寻找最后一个在指定分值范围内的节点
     *
     * @param minScore
     * @param maxScore
     * @return
     */
    public SkipListNode findLastInScoreRange(int minScore, int maxScore) {
        if (header.forwards[0] == null) {
            return null;
        }
        //如果最后一个节点刚刚好在这个区间，那就直接返回
        SkipListNode first = header.forwards[0].getForward();
        if (tail.getScore() >= minScore && tail.getScore() <= maxScore) {
            return header;
        } else if (first.getScore() > maxScore || tail.getScore() < minScore) {
            return null;
        }

        SkipListNode p = header;
        for (int i = level - 1; i >= 0; i--) {
            SkipListLevel skipListLevel = null;
            while ((skipListLevel = p.forwards[i]) != null) {
                SkipListNode next = skipListLevel.getForward();
                //在最大值范围内，比之前大
                if (next.getScore() <= maxScore) {
                    p = next;
                    continue;
                }
                break;
            }
        }

        if (p.getScore() >= minScore && p.getScore() <= maxScore) {
            return p;
        }
        return null;
    }

    /**
     * 若成员相同，返回0，不替换，否则返回1
     *
     * @param score
     * @param member
     * @return
     */
    public int insert(int score, Sds member) {

        if (findScoreByMember(member) != null) {
            return 0;
        }

        /**
         * 从header开始寻找插入位置
         */
        SkipListNode[] update = new SkipListNode[MAX_LEVEL];
        int[] updateNodeSpan = new int[MAX_LEVEL];
        SkipListNode p = header;
        for (int i = level - 1; i >= 0; i--) {
            /**
             * 当前level，寻找一个符合以下条件之一的作为前继节点
             * 1. 后面为null
             * 2. 面节点>要插入的score || 分值相同，但是后面的节点>=要插入的member（升序排列）
             */
            updateNodeSpan[i] = i < level - 1 ? updateNodeSpan[i + 1] : 0;
            SkipListLevel skipListLevel = null;
            while ((skipListLevel = p.forwards[i]) != null) {
                SkipListNode next = skipListLevel.forward;
                if (next.score < score
                        || (next.score == score && next.member.compareTo(member) == -1)) {
                    updateNodeSpan[i] += skipListLevel.getSpan(); //寻找的过程也是累加的过程
                    p = next;
                } else {
                    break;
                }
            }
            update[i] = p;
        }

        int newLevel = randomLevel();
        if (newLevel > level) {
            for (int i = level; i < newLevel; i++) {
                update[i] = header; //超过的部分给定头结点
            }

            level = newLevel;
        }

        /**
         * 1. 更新新节点forward指向原前继的forward节点
         * 2. 更新前继节点的forward指针指向新节点
         */
        SkipListNode newNode = new SkipListNode(p, newLevel, score, member);
        if (p.forwards[0] != null) { //后退指针赋值
            p.forwards[0].getForward().backward = newNode;
        } else {
            tail = newNode;
        }
        /**
         * 1. 超过newLevel部分，且不为null，span要+1
         * 2. 未超过，但是==null，update的forward指针赋值span
         * 3. 未超过，但是不为null，细分以下2种情况
         * 4. 如果newLevel比前面节点高，那可能出现原本是跨度>1的情况
         * (1) 指针正常赋值，新节点和前继
         * (2)
         * 5. 剩余部分，直接用就行了
         */
        for (int i = 0; i < level; i++) {
            if (i >= newLevel && update[i].forwards[i] != null) {
                update[i].forwards[i].span++;
            } else if (i < newLevel && update[i].forwards[i] == null) {
                update[i].forwards[i] = new SkipListLevel(newNode, updateNodeSpan[0] - updateNodeSpan[i] + 1);
            } else if (i < newLevel && update[i].forwards[i] != null) {
                int newSpan = updateNodeSpan[0] - updateNodeSpan[i] + 1;
                newNode.forwards[i] = update[i].forwards[i];
                newNode.forwards[i].span = newNode.forwards[i].span - newSpan + 1;
                update[i].forwards[i] = new SkipListLevel(newNode, newSpan);
            } else {
                //ignore
            }
        }

        member2Score.put(member, score);

        ++len;

        return 1;
    }

    /**
     * 删除指定元素，删除就是插入的逆向：
     * 1. 和插入一样，先初始化update向量，然后断开forward指针
     * 2. 遍历header判断是否缩减level
     *
     * @param member
     * @return
     */
    public int delete(Sds member) {

        Integer score = member2Score.get(member);
        if (score == null) {
            return 0;
        }

        //寻找
        SkipListNode[] update = new SkipListNode[MAX_LEVEL];
        SkipListNode p = header;
        for (int i = level - 1; i >= 0; i--) {
            SkipListLevel skipListLevel = null;
            while ((skipListLevel = p.forwards[i]) != null) {
                SkipListNode next = skipListLevel.getForward();
                if (next.score <= score && next.member.compareTo(member) == -1) {
                    p = next;
                } else {
                    break;
                }
            }
            update[i] = p;
        }

        p = p.forwards[0].getForward();
        if (p != null && p.member.compareTo(member) == 0) { //相同
            if (tail == p) { //最后一个
                tail = p.backward;
            }

            for (int i = 0; i < level; i++) {
                //不等说明到头了，后面跨度就是>1，这里减1就行了
                if (update[i].forwards[i].getForward() != p) {
                    update[i].forwards[i].span--;
                } else {
                    update[i].forwards[i].span += (p.forwards[i] != null ? p.forwards[i].span : 0);
                    update[i].forwards[i] = p.forwards[i];
                }
            }
            --len;
            free(p);
            member2Score.remove(member);

            //如果header的跳步就到null，说明这一层没什么用了
            while (level > 1 && header.forwards[level - 1] == null) {
                --level;
            }

            return 1;
        }
        return 0;
    }

    /**
     * 批量删除 ZREM
     *
     * @param members
     * @return
     */
    public int deleteBatch(List<Sds> members) {
        int result = 0;
        for (Sds member : members) {
            result += delete(member);
        }
        return result;
    }

    /**
     * 根据分数范围在这其中的节点
     *
     * @param minScore
     * @param maxScore
     * @return
     */
    public int deleteByScoreRange(int minScore, int maxScore) {

        //先找到小于min的第一个
        SkipListNode[] update = new SkipListNode[MAX_LEVEL];
        SkipListNode p = header;
        for (int i = level - 1; i >= 0; i--) {
            SkipListLevel skipListLevel = null;
            while ((skipListLevel = p.forwards[i]) != null) {
                SkipListNode next = skipListLevel.getForward();
                //小于最小值，向右寻找
                if (next.getScore() < minScore) {
                    p = next;
                    continue;
                }
                break;
            }
            update[i] = p;
        }

        int delCount = 0;
        p = p.forwards[0].getForward();
        while (p != null) {
            if (p.getScore() < minScore || p.getScore() > maxScore) {
                break;
            }

            if (tail == p) { //最后一个
                tail = p.backward;
            }

            for (int i = 0; i < level; i++) {
                //不等说明到头了，后面跨度就是>1，这里减1就行了
                if (update[i].forwards[i].getForward() != p) {
                    update[i].forwards[i].span--;
                } else {
                    update[i].forwards[i].span += (p.forwards[i] != null ? p.forwards[i].span : 0);
                    update[i].forwards[i] = p.forwards[i];
                }
            }
            --len;
            //先记录要删除节点
            SkipListNode delNode = p;
            //需要递推下一个
            p = p.forwards[0].getForward();
            //最后释放
            free(delNode);
            member2Score.remove(p.getMember());

            //如果header的跳步就到null，说明这一层没什么用了
            while (level > 1 && header.forwards[level - 1] == null) {
                --level;
            }
            ++delCount;
        }

        return delCount;
    }

    /**
     * 根据排位范围在这其中的节点
     *
     * @param minRank
     * @param maxRank
     * @return
     */
    public int deleteByRankRange(int minRank, int maxRank) {

        //先找到小于min的第一个
        int span = 0;
        SkipListNode[] update = new SkipListNode[MAX_LEVEL];
        SkipListNode p = header;
        for (int i = level - 1; i >= 0; i--) {
            SkipListLevel skipListLevel = null;
            while ((skipListLevel = p.forwards[i]) != null) {
                SkipListNode next = skipListLevel.getForward();
                //小于最小值，向右寻找
                if (span + skipListLevel.getSpan() < minRank) {
                    p = next;
                    span += skipListLevel.getSpan();
                    continue;
                }
                break;
            }
            update[i] = p;
        }

        int delCount = 0;
        p = p.forwards[0].getForward();
        while (p != null) {
            if (p.getScore() < minRank || p.getScore() > maxRank) {
                break;
            }

            if (tail == p) { //最后一个
                tail = p.backward;
            }

            for (int i = 0; i < level; i++) {
                //不等说明到头了，后面跨度就是>1，这里减1就行了
                if (update[i].forwards[i].getForward() != p) {
                    update[i].forwards[i].span--;
                } else {
                    update[i].forwards[i].span += (p.forwards[i] != null ? p.forwards[i].span : 0);
                    update[i].forwards[i] = p.forwards[i];
                }
            }
            --len;
            //先记录要删除节点
            SkipListNode delNode = p;
            //需要递推下一个
            p = p.forwards[0].getForward();
            //最后释放
            free(delNode);
            member2Score.remove(p.getMember());

            //如果header的跳步就到null，说明这一层没什么用了
            while (level > 1 && header.forwards[level - 1] == null) {
                --level;
            }
            ++delCount;
        }

        return delCount;
    }

    /**
     * ZCARED
     *
     * @return
     */
    public int len() {
        return len;
    }

    private void free(SkipListNode node) {
        for (int i = 0; i < level && i < node.forwards.length; i++) {
            if (node.forwards[i] != null) {
                node.forwards[i].forward = null;
                node.forwards[i] = null;
            }
        }
        node.backward = null;
        node.forwards = null;
        node.member = null;
    }

    /**
     * 随机出索引层高度
     *
     * @return
     */
    private int randomLevel() {
        int level = 1;
        while (Math.random() < 0.5 && level < MAX_LEVEL) {
            ++level;
        }
        return level;
    }

    public static class SkipListLevel {
        private SkipListNode forward;
        private int span;

        public SkipListLevel(SkipListNode forward, int span) {
            this.forward = forward;
            this.span = span;
        }

        public SkipListNode getForward() {
            return forward;
        }

        public int getSpan() {
            return span;
        }
    }

    /**
     * 跳表节点
     */
    public static class SkipListNode {
        private SkipListLevel[] forwards;
        private SkipListNode backward;

        private double score; //分值
        private Sds member;

        public SkipListNode() {
            this.forwards = new SkipListLevel[MAX_LEVEL];
        }

        public SkipListNode(SkipListNode backward, int level, double score, Sds member) {
            this.backward = backward;
            this.forwards = new SkipListLevel[level];
            this.score = score;
            this.member = member;
        }

        public SkipListNode getBackward() {
            return backward;
        }

        public double getScore() {
            return score;
        }

        public Sds getMember() {
            return member;
        }

        @Override
        public String toString() {
            return "Node@" + hashCode();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        int count = 0;
        SkipListNode p = header;
        while (p != null && p.forwards[0] != null) {
            sb.append("第").append(count).append("个").append("|");
            sb.append("score=").append(p.score).append("|");
            sb.append("levelTotal=").append(p.forwards.length).append("|");
            for (int i = 0; i < level && i < p.forwards.length; i++) {
                if (p.forwards[i] == null) break;
                sb.append("[level=").append(i);
                sb.append(", span=").append(p.forwards[i].getSpan());
                sb.append(", forward=").append(p.forwards[i].getForward());
                sb.append("]");
            }
            sb.append("\n");
            ++count;
            p = p.forwards[0].forward;
        }

        return sb.toString();
    }
}
