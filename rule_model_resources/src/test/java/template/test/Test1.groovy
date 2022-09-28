//package template.test
//
//import cn.doitedu.rtmk.common.interfaces.RuleCalculator
//import cn.doitedu.rtmk.common.pojo.UserEvent
//import com.alibaba.fastjson.JSONArray
//import com.alibaba.fastjson.JSONObject
//import org.roaringbitmap.RoaringBitmap
//import redis.clients.jedis.Jedis
//
//class Test1 implements RuleCalculator {
//    private Jedis jedis;
//
//    // 规则的参数整体json对象
//    private JSONObject ruleDefineParamJsonObject;
//
//    // 行为次数条件
//    private JSONObject eventCountConditionParam;
//
//    // 行为序列条件
//    private JSONObject actionSeqCondition;
//
//    // 行为序列条件中的事件定义数组
//    JSONArray eventParams;
//
//    // 行为序列的条件id
//    private int actionSeqConditionId;
//
//    // 规则id
//    private String ruleId;
//
//    // 规则的触达次数上限
//    int ruleMatchMaxCount;
//
//    // 人群画像圈选bitmap
//    private RoaringBitmap profileUserBitmap;
//
//    @Override
//    void init(JSONObject ruleDefineParamJsonObject, RoaringBitmap profileUserBitmap) {
//        this.jedis = new Jedis("localhost",6379);
//        this.ruleDefineParamJsonObject = ruleDefineParamJsonObject;
//        this.profileUserBitmap = profileUserBitmap;
//
//
//        // 取到规则的标识id
//        this.ruleId = ruleDefineParamJsonObject.getString("ruleId");
//        this.ruleMatchMaxCount = ruleDefineParamJsonObject.getInteger("rule_match_count");
//
//        // 取到规则的事件次数条件
//        this.eventCountConditionParam = ruleDefineParamJsonObject.getJSONObject("actionCountCondition");
//        this.actionSeqCondition = ruleDefineParamJsonObject.getJSONObject("actionSeqCondition");
//        this.actionSeqConditionId = actionSeqCondition.getInteger("conditionId")
//        this.eventParams = actionSeqCondition.getJSONArray("eventParams")
//    }
//
//    @Override
//    List<JSONObject> process(UserEvent userEvent) {
//        List<JSONObject> resLst = new ArrayList<JSONObject>();
//
//        // 判断本事件的行为人，是否属于本规则的画像人群
//        if (profileUserBitmap.contains(userEvent.getGuid())) {
//
//            // 取出规则的触发事件条件参数json
//            JSONObject ruleTrigEventJsonObject = ruleDefineParamJsonObject.getJSONObject("ruleTrigEvent");
//
//            // 判断用户行为事件，如果本事件是规则的触发条件，则进行规则的匹配判断
//            if (UserEventComparator.userEventIsEqualParam(userEvent, ruleTrigEventJsonObject)) {
//                JSONObject resObj1 = new JSONObject();
//                resObj1.put("ruleId",ruleId);
//                resObj1.put("resType","trigger");
//                resObj1.put("guid",userEvent.getGuid());
//                resObj1.put("timestamp",System.currentTimeMillis());
//                resLst.add(resObj1);
//
//                // 如果是触发事件，则判断本行为人是否已经满足了本规则的所有条件
//                boolean isMatch = isMatch(userEvent.getGuid());
//
//                log.debug("用户:{} ,触发事件:{},规则:{},规则匹配结果:{}", userEvent.getGuid(), userEvent.getEventId(), ruleId, isMatch);
//
//                // 如果已满足，则输出本规则的触达信息
//                if (isMatch) {
//                    JSONObject resObj2 = new JSONObject();
//                    resObj2.put("ruleId",ruleId);
//                    resObj2.put("resType","match");
//                    resObj2.put("guid",userEvent.getGuid());
//                    resObj2.put("timestamp",System.currentTimeMillis());
//                    resLst.add(resObj2);
//                }
//            }
//            // 判断用户行为事件，如果本事件不是规则的触发事件，则进行规则的条件统计运算
//            else {
//                // 做规则条件的统计运算
//                calc(userEvent);
//                log.debug("收到用户:{} ,行为事件:{}, 规则条件运算：{}", userEvent.getGuid(), userEvent.getEventId(), ruleId);
//                JSONObject resObj = new JSONObject();
//                resObj.put("ruleId",ruleId);
//                resObj.put("resType","calc");
//                resObj.put("guid",userEvent.getGuid());
//                resObj.put("timestamp",System.currentTimeMillis());
//                resLst.add(resObj);
//            }
//        }
//        return resLst;
//    }
//
//    @Override
//    void calc(UserEvent userEvent) {
//        // 行为次数类条件的事件运算
//        calcActionCount(userEvent)
//        // 行为序列条件的事件运算
//        caclActionSeq(userEvent)
//
//    }
//    /**
//     * 规则条件的行为次数条件实时运算逻辑
//     *
//     * @param userEvent 输入的一次用户行为
//     */
//    void calcActionCount(UserEvent userEvent) {
//
//        long eventTime = userEvent.getEventTime()
//
//        JSONArray eventParams = eventCountConditionParam.getJSONArray("eventParams");
//        int size = eventParams.size();
//
//        // 遍历事件次数条件中的每一个事件参数
//        for (int i = 0; i < size; i++) {
//
//            // 取出事件条件列表中的：一个事件条件参数
//            JSONObject eventParam = eventParams.getJSONObject(i);
//
//            // "2022-08-01 12:00:00"
//            String windowStart = eventParam.getString("windowStart")
//            String windowEnd = eventParam.getString("windowEnd")
//            long startTime = DateUtils.parseDate(windowStart, "yyyy-MM-dd HH:mm:ss").getTime()
//            long endTime = DateUtils.parseDate(windowEnd, "yyyy-MM-dd HH:mm:ss").getTime()
//
//            // 1.先判断输入的用户行为事件，是否处于规则参数约定的计算时间窗口内
//            if (eventTime >= startTime && eventTime <= endTime) {
//                // 2. 判断当前输入的事件是否匹配条件参数中要求的事件
//                if (UserEventComparator.userEventIsEqualParam(userEvent, eventParam)) {
//                    // 取出本条件的条件id
//                    Integer conditionId = eventParam.getInteger("conditionId");
//
//                    // 如果是，就需要去redis中，给这个用户的，这个规则的，这个条件的次数+1
//                    jedis.hincrBy(ruleId + ":" + conditionId, userEvent.getGuid() + "", 1);
//                }
//            }
//        }
//    }
//
//    /**
//     * 规则条件的行为序列条件实时运算逻辑
//     * @param userEvent
//     */
//    void caclActionSeq(UserEvent userEvent) {
//        int guid = userEvent.getGuid()
//        String redisSeqStepKey = ruleId + ":" + actionSeqConditionId + ":step";
//        String redisSeqCntKey = ruleId + ":" + actionSeqConditionId + ":cnt";
//
//        // 1. 从redis中获取该用户的，本规则的行为序列的，待完成序列的，已到达的，步骤号
//        String preStepStr = jedis.hget(redisSeqStepKey, guid + "")
//        int preStep = preStepStr == null ? 0 : Integer.parseInt(preStepStr)
//
//        // A-C-D
//        // 判断本次输入的事件，是否是行为序列参数期待的下一个事件
//        JSONObject eventParam = eventParams.getJSONObject(preStep)
//
//        // "2022-08-01 12:00:00"
//        String windowStart = actionSeqCondition.getString("windowStart")
//        String windowEnd = actionSeqCondition.getString("windowEnd")
//        long startTime = DateUtils.parseDate(windowStart, "yyyy-MM-dd HH:mm:ss").getTime()
//        long endTime = DateUtils.parseDate(windowEnd, "yyyy-MM-dd HH:mm:ss").getTime()
//
//
//        // 1.先判断输入的用户行为事件，是否处于规则参数约定的计算时间窗口内
//        if (userEvent.getEventTime() >= startTime && userEvent.getEventTime() <= endTime) {
//            // 2.如果输入事件，正是待完成序列所期待的事件
//            if (UserEventComparator.userEventIsEqualParam(userEvent, eventParam)) {
//                if (preStep == eventParams.size()-1 ) {
//                    // 将redis中的步骤号重置为0
//                    jedis.hset(redisSeqStepKey, guid + "", "0")
//                    // 并且将redis中该用户该条件的完成次数+1
//                    jedis.hincrBy(redisSeqCntKey, guid + "", 1)
//                } else {
//                    // 否则步骤号+1
//                    long by = jedis.hincrBy(redisSeqStepKey, guid + "", 1)
//                }
//            }
//        }
//
//    }
//
//    @Override
//    boolean isMatch(int guid) {
//        String redisMatchCntKey = ruleId+":"+"mcnt"
//        // 取出本规则，该用户实际触达的次数
//        String ruleMatchRealCntStr = jedis.hget(redisMatchCntKey, guid+"")
//        int ruleMatchRealCnt = ruleMatchRealCntStr == null ? 0 : Integer.parseInt(ruleMatchRealCntStr)
//
//        if(ruleMatchRealCnt <= ruleMatchMaxCount -1) {
//
//            boolean res_0 = isMatchEventCount(guid)
//            boolean res_1 = isMatchEventSeq(guid)
//            if( #(ruleCombineExpr) ){
//                jedis.hincrBy(redisMatchCntKey,guid+"",1)
//                return true;
//            }
//        }
//        return  false;
//    }
//    boolean isMatchEventCount(int guid) {
//        JSONArray eventParams = eventCountConditionParam.getJSONArray("eventParams");
//
//        #for(eventParam : eventParams)
//            JSONObject eventParam_#(for.index) = eventParams.getJSONObject(#(for.index));
//        Integer conditionId_#(for.index) = eventParam_#(for.index).getInteger("conditionId");
//
//        Integer eventCountParam_#(for.index) = eventParam_#(for.index).getInteger("eventCount");
//
//        String realCountStr_#(for.index) = jedis.hget(ruleId + ":" + conditionId_#(for.index), guid + "");
//        int realCount_#(for.index) = Integer.parseInt(realCountStr_#(for.index) == null ? "0" : realCountStr_#(for.index));
//
//        boolean res_#(for.index) = realCount_#(for.index) >= eventCountParam_#(for.index) ;
//
//        #end
//
//        return  #(cntConditionCombineExpr);
//    }
//
//    boolean isMatchEventSeq(int guid) {
//        int seqCountParam = actionSeqCondition.getInteger("seqCount")
//        String redisSeqCntKey = ruleId + ":" + actionSeqConditionId + ":cnt";
//        String seqRealCountStr = jedis.hget(redisSeqCntKey, guid + "")
//        int seqRealCount = seqRealCountStr == null ? 0: Integer.parseInt(seqRealCountStr)
//
//        return seqRealCount>=seqCountParam;
//    }
//}
