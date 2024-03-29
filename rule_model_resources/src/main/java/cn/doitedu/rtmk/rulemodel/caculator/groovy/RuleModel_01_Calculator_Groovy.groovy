package cn.doitedu.rtmk.rulemodel.caculator.groovy

import cn.doitedu.rtmk.common.interfaces.RuleCalculator
import cn.doitedu.rtmk.common.pojo.UserEvent
import cn.doitedu.rtmk.common.utils.UserEventComparator
import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import groovy.util.logging.Slf4j
import org.apache.commons.lang3.time.DateUtils
import org.apache.flink.util.Collector
import org.roaringbitmap.RoaringBitmap
import redis.clients.jedis.Jedis

/**
 * 规则运算机的：规则模型01实现类
 */
@Slf4j
class RuleModel_01_Calculator_Groovy implements RuleCalculator {

    private Jedis jedis;
    private JSONObject ruleDefineParamJsonObject;
    private JSONObject eventCountConditionParam;
    private String ruleId;
    private RoaringBitmap profileUserBitmap;
    private Collector<JSONObject> out;
    JSONObject resultObject;


    /**
     * 规则运算机的初始化方法
     *
     * @param jedis 连接redis的客户端
     * @param ruleDefineParamJsonObject 整个规则的json参数
     */
    @Override
    void init(JSONObject ruleDefineParamJsonObject, RoaringBitmap profileUserBitmap) {
        this.jedis = jedis;
        this.ruleDefineParamJsonObject = ruleDefineParamJsonObject;
        this.profileUserBitmap = profileUserBitmap;
        this.out = out;

        // 取到规则的标识id
        ruleId = ruleDefineParamJsonObject.getString("ruleId");

        // 取到规则的事件次数条件
        this.eventCountConditionParam = ruleDefineParamJsonObject.getJSONObject("actionCountCondition");

        // 构造一个匹配结果输出对象
        resultObject = new JSONObject();
        resultObject.put("ruleId", ruleId)

    }


    /**
     * 输入事件的规则处理入口方法
     * @param userEvent 输入的用户行为事件
     */
    @Override
    List<JSONObject> process(UserEvent userEvent) {

        // 判断本事件的行为人，是否属于本规则的画像人群
        if (profileUserBitmap.contains(userEvent.getGuid())) {

            // 取出规则的触发事件条件参数json
            JSONObject ruleTrigEventJsonObject = ruleDefineParamJsonObject.getJSONObject("ruleTrigEvent");

            // 判断用户行为事件，如果本事件是规则的触发条件，则进行规则的匹配判断
            if (UserEventComparator.userEventIsEqualParam(userEvent, ruleTrigEventJsonObject)) {

                // 如果是触发事件，则判断本行为人是否已经满足了本规则的所有条件
                boolean isMatch = isMatch(userEvent.getGuid());

                log.info("用户:{} ,触发事件:{},规则:{},规则匹配结果:{}",userEvent.getGuid(),userEvent.getEventId(),ruleId,isMatch);

                // 如果已满足，则输出本规则的触达信息
                if (isMatch) {
                    resultObject.put("guid", userEvent.getGuid());
                    resultObject.put("matchTime", System.currentTimeMillis())
                    out.collect(resultObject);
                }
            }
            // 判断用户行为事件，如果本事件不是规则的触发事件，则进行规则的条件统计运算
            else {
                // 做规则条件的统计运算
                calc(userEvent);
                log.info("收到用户:{} ,行为事件:{}, 规则条件运算：{}", userEvent.getGuid(), userEvent.getEventId(), ruleId);
            }
        }
    }


    /**
     * 规则条件的实时运算逻辑
     *
     * @param userEvent 输入的一次用户行为
     */
    @Override
    public void calc(UserEvent userEvent) {

        long eventTime = userEvent.getEventTime()

        JSONArray eventParams = eventCountConditionParam.getJSONArray("eventParams");
        int size = eventParams.size();

        // 遍历事件次数条件中的每一个事件参数
        for (int i = 0; i < size; i++) {

            // 取出事件条件列表中的：一个事件条件参数
            JSONObject eventParam = eventParams.getJSONObject(i);

            // "2022-08-01 12:00:00"
            String windowStart = eventParam.getString("windowStart")
            String windowEnd = eventParam.getString("windowEnd")
            long startTime = DateUtils.parseDate(windowStart, "yyyy-MM-dd HH:mm:ss").getTime()
            long endTime = DateUtils.parseDate(windowEnd, "yyyy-MM-dd HH:mm:ss").getTime()

            // 1.先判断输入的用户行为事件，是否处于规则参数约定的计算时间窗口内
            if (eventTime >= startTime && eventTime <= endTime) {
                log.info("用户输入事件的时间，符合参数窗口要求")

                // 取出本条件的条件id
                Integer conditionId = eventParam.getInteger("conditionId");

                // 2. 判断当前输入的事件是否匹配条件参数中要求的事件
                if(UserEventComparator.userEventIsEqualParam(userEvent,eventParam)){
                    log.info("用户输入事件，与规则条件的事件参数吻合，即将更新redis的计算结果")


                    // 如果是，就需要去redis中，给这个用户的，这个规则的，这个条件的次数+1
                    jedis.hincrBy(ruleId + ":" + conditionId, userEvent.getGuid() + "", 1);
                }

            }

        }


    }


    /**
     * 判断某用户是否满足了该条件
     *
     * @param guid 用户id
     * @return 是否满足该条件
     * <p>
     * " res0 && (res1 || res2) "
     * " res0 && res2  || res3 && res4 "
     * " res0 || res1 "
     * " res0 && res1 && res2 "
     */
    @Override
    public boolean isMatch(int guid) {
        JSONArray eventParams = eventCountConditionParam.getJSONArray("eventParams");


        // 取出一个事件条件参数
        JSONObject eventParam_0 = eventParams.getJSONObject(0);
        // 取出该条件的条件id
        Integer conditionId_0 = eventParam_0.getInteger("conditionId");

        // 取出该事件次数条件的要求的发生次数
        Integer eventCountParam_0 = eventParam_0.getInteger("eventCount");

        // 去redis中查询该用户，该条件的实际发生次数
        String realCountStr_0 = jedis.hget(ruleId + ":" + conditionId_0, guid + "");
        int realCount_0 = Integer.parseInt(realCountStr_0 == null ? "0" : realCountStr_0);

        // 判断，该条件是否已经满足
        boolean res_0 = realCount_0 >= eventCountParam_0;




        return res_0;
    }
}