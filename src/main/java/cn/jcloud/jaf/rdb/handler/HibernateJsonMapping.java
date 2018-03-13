package cn.jcloud.jaf.rdb.handler;

import org.hibernate.engine.spi.Mapping;
import org.hibernate.id.factory.IdentifierGeneratorFactory;
import org.hibernate.type.Type;

/**
 * Hibernate Json序列化时,Prox类型的id字段名映射
 * Created by wei Han on 2016/2/23.
 */
public class HibernateJsonMapping implements Mapping {
    @Override
    public IdentifierGeneratorFactory getIdentifierGeneratorFactory() {
        return null;
    }

    @Override
    public Type getIdentifierType(String className) {
        return null;
    }

    @Override
    public String getIdentifierPropertyName(String className) {
        //这里写使用硬编码的方式来获取id字段名，后续如需要再扩展实现
        return "id";
    }

    @Override
    public Type getReferencedPropertyType(String className, String propertyName) {
        return null;
    }
}
