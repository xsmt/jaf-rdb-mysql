package cn.jcloud.jaf.rdb.base.repository;

import cn.jcloud.jaf.common.constant.ErrorCode;
import cn.jcloud.jaf.common.exception.JafI18NException;
import cn.jcloud.jaf.common.query.Condition;
import cn.jcloud.jaf.common.query.Items;
import cn.jcloud.jaf.common.query.ListParam;
import org.springframework.data.domain.Sort;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.criteria.*;
import java.util.*;

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
            String[] fieldArray = condition.getField().split("\\.");
            query.setParameter(fieldArray[fieldArray.length-1] + i, value);
        }
    }


    private static <T> Predicate buildPredicate(CriteriaBuilder builder, Root<T> root, Condition condition, int index) {
        String[] fieldArray = condition.getField().split("\\.");
        Class valueType = condition.getValueType();
        Predicate predicate;
        int fieldIndex = 0;
        Path path = root.get(fieldArray[fieldIndex]);
        Class classType = path.getJavaType();
        if (classType.equals(Set.class)) {
            SetJoin setJoin = root.joinSet(fieldArray[fieldIndex]);
            if (++fieldIndex >= fieldArray.length) {
                throw JafI18NException.of(ErrorCode.INVALID_QUERY);
            }
            path = setJoin.get(fieldArray[fieldIndex]);
        } else if (classType.equals(List.class)) {
            ListJoin listJoin = root.joinList(fieldArray[fieldIndex]);
            if (++fieldIndex >= fieldArray.length) {
                throw JafI18NException.of(ErrorCode.INVALID_QUERY);
            }
            path = listJoin.get(fieldArray[fieldIndex]);
        } else if (classType.equals(Map.class)) {
            MapJoin mapJoin = root.joinMap(fieldArray[fieldIndex]);
            if (++fieldIndex >= fieldArray.length) {
                throw JafI18NException.of(ErrorCode.INVALID_QUERY);
            }
            path = mapJoin.get(fieldArray[fieldIndex]);
        }
        while (++fieldIndex < fieldArray.length) {
            path = path.get(fieldArray[fieldIndex]);
        }

        String parameterName = fieldArray[fieldArray.length-1] + index;

        switch (condition.getOperator()) {
            case EQ:
                predicate = builder.equal(
                        path,
                        builder.parameter(valueType, parameterName)
                );
                break;
            case NE:
                predicate = builder.notEqual(
                        path,
                        builder.parameter(valueType, parameterName)
                );
                break;
            case GT:
                predicate = builder.greaterThan(
                        path.as(valueType),
                        builder.parameter(valueType, parameterName)
                );
                break;
            case GE:
                predicate = builder.greaterThanOrEqualTo(
                        path.as(valueType),
                        builder.parameter(valueType, parameterName)
                );
                break;
            case LT:
                predicate = builder.lessThan(
                        path.as(valueType),
                        builder.parameter(valueType, parameterName)
                );
                break;
            case LE:
                predicate = builder.lessThanOrEqualTo(
                        path.as(valueType),
                        builder.parameter(valueType, parameterName)
                );
                break;
            case LIKE:
                predicate = builder.like(
                        path.as(String.class),
                        builder.parameter(String.class, parameterName)
                );
                break;
            default:
                throw JafI18NException.of("非法或不支持的操作符", ErrorCode.INVALID_QUERY);
        }
        return predicate;
    }
}
