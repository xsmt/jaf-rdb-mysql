package cn.jcloud.jaf.rdb.base.repository;

import cn.jcloud.jaf.common.base.domain.BaseDomain;
import cn.jcloud.jaf.common.constant.ErrorCode;
import cn.jcloud.jaf.common.exception.JafI18NException;
import cn.jcloud.jaf.common.query.Condition;
import cn.jcloud.jaf.common.query.Items;
import cn.jcloud.jaf.common.query.ListParam;
import cn.jcloud.jaf.common.util.ReflectUtil;
import org.springframework.data.domain.Sort;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.criteria.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 与ListParam相关的Jpa查询工具类
 * Created by Wei Han on 2016/2/17.
 */
public class ListParamJpaUtil {

    private ListParamJpaUtil() {
    }

    public static <T> Items<T> list(EntityManager em, ListParam<T> listParam, Class<T> domainType) {
        List<T> list = queryList(em, listParam, domainType);
        if (!listParam.isCount()) {
            return Items.of(list);
        }
        long count = getCount(em, listParam, domainType);
        return Items.of(list, count);
    }

    public static <T> List<T> queryList(EntityManager em, ListParam<T> listParam,
                                        Class<T> domainType) {
        CriteriaBuilder builder = em.getCriteriaBuilder();
        CriteriaQuery<T> criteriaQuery = builder.createQuery(domainType);
        Root<T> root = criteriaQuery.from(domainType);
        criteriaQuery.select(root);
        buildPredicates(builder, criteriaQuery, root, listParam.getConditions());
        buildOrders(builder, criteriaQuery, root, listParam.getSort());
        Query query = em.createQuery(criteriaQuery);
        buildParameter(listParam.getConditions(), query);
        query.setFirstResult(listParam.getOffset());
        query.setMaxResults(listParam.getLimit());
        return query.getResultList();
    }

    public static <T> Long getCount(EntityManager em, ListParam<T> listParam, Class<T> domainType) {
        CriteriaBuilder builder = em.getCriteriaBuilder();
        CriteriaQuery<Long> criteriaQuery = builder
                .createQuery(Long.class);
        Root<T> root = criteriaQuery.from(domainType);
        criteriaQuery.select(builder.count(root));

        //设置查询条件
        List<Condition> conditions = listParam.getConditions();
        buildPredicates(builder, criteriaQuery, root, conditions);

        //设置查询参数
        Query query = em.createQuery(criteriaQuery);
        buildParameter(conditions, query);
        return (Long) query.getSingleResult();
    }

    private static <T, R> void buildOrders(CriteriaBuilder builder, CriteriaQuery<R> criteriaQuery, Root<T> root, Sort sort) {
        if (sort == null) {
            return;
        }
        List<Order> orders = new LinkedList<>();
        for (Sort.Order sortOrder : sort) {
            Order order;
            if (Sort.Direction.ASC.equals(sortOrder.getDirection())) {
                order = builder.asc(root.get(sortOrder.getProperty()));
            } else {
                order = builder.desc(root.get(sortOrder.getProperty()));
            }
            orders.add(order);
        }
        Order[] orderArr = new Order[orders.size()];
        orders.toArray(orderArr);
        criteriaQuery.orderBy(orderArr);
    }

    public static <T> void buildPredicates(CriteriaBuilder builder, CriteriaQuery criteriaQuery,
                                           Root<T> root, List<Condition> conditions) {
        if (conditions.isEmpty()) {
            return;
        }
        List<Predicate> predicates = new ArrayList<>(conditions.size());
        for (int i = 0, size = conditions.size(); i < size; i++) {
            Condition condition = conditions.get(i);
            predicates.add(buildPredicate(builder, root, condition, i));
        }
        Predicate[] predicateArr = new Predicate[predicates.size()];
        predicates.toArray(predicateArr);
        criteriaQuery.where(builder.and(predicateArr));
    }

    public static void buildParameter(List<Condition> conditions, Query query) {
        for (int i = 0, size = conditions.size(); i < size; i++) {
            Condition condition = conditions.get(i);
            Object value = condition.getValue();
            Class valueType = condition.getValueType();
            value = getRealTypeValue(value, valueType);
            query.setParameter(condition.getField() + i, value);
        }
    }

    private static Object getRealTypeValue(Object value, Class valueType) {
        if (BaseDomain.class.isAssignableFrom(valueType)) {
            try {
                BaseDomain domain = (BaseDomain) valueType.newInstance();
                if (Long.class.equals(ReflectUtil.getGenericParameter(valueType)[0])) {
                    domain.setId(Long.parseLong(String.valueOf(value)));
                } else {
                    domain.setId((Serializable) value);
                }
                return domain;
            } catch (InstantiationException | IllegalAccessException e) {
                throw JafI18NException.of("无法实例类型" + valueType.getCanonicalName()
                        + ",可能由于缺少无参构造函数", ErrorCode.INVALID_ARGUMENT, e);
            }
        }
        return value;
    }

    private static <T> Predicate buildPredicate(CriteriaBuilder builder, Root<T> root, Condition condition, int index) {
        String field = condition.getField();
        String parameterName = field + index;
        Class valueType = condition.getValueType();
        Predicate predicate;
        switch (condition.getOperator()) {
            case EQ:
                predicate = builder.equal(
                        root.get(field),
                        builder.parameter(valueType, parameterName)
                );
                break;
            case NE:
                predicate = builder.notEqual(
                        root.get(field),
                        builder.parameter(valueType, parameterName)
                );
                break;
            case GT:
                predicate = builder.greaterThan(
                        root.get(field).as(valueType),
                        builder.parameter(valueType, parameterName)
                );
                break;
            case GE:
                predicate = builder.greaterThanOrEqualTo(
                        root.get(field).as(valueType),
                        builder.parameter(valueType, parameterName)
                );
                break;
            case LT:
                predicate = builder.lessThan(
                        root.get(field).as(valueType),
                        builder.parameter(valueType, parameterName)
                );
                break;
            case LE:
                predicate = builder.lessThanOrEqualTo(
                        root.get(field).as(valueType),
                        builder.parameter(valueType, parameterName)
                );
                break;
            case LIKE:
                predicate = builder.like(
                        root.get(field).as(String.class),
                        builder.parameter(String.class, parameterName)
                );
                break;
            default:
                throw JafI18NException.of("非法或不支持的操作符", ErrorCode.INVALID_QUERY);
        }
        return predicate;
    }
}
