package cn.jcloud.jaf.rdb.base.repository;

import cn.jcloud.jaf.common.base.domain.BaseDomain;
import cn.jcloud.jaf.common.base.repository.BaseRepository;
import cn.jcloud.jaf.common.query.Items;
import cn.jcloud.jaf.common.query.ListParam;
import org.springframework.data.jpa.repository.support.JpaEntityInformation;
import org.springframework.data.jpa.repository.support.SimpleJpaRepository;

import javax.persistence.EntityManager;
import java.io.Serializable;

/**
 * Dao基础实现
 * Created by Wei Han on 2016/1/19.
 */
public class BaseRepositoryImpl<T extends BaseDomain<I>, I extends Serializable> extends SimpleJpaRepository<T, I>
        implements BaseRepository<T, I> {

    private final EntityManager em;
    private final Class<T> domainType;

    public BaseRepositoryImpl(JpaEntityInformation<T, I> entityInformation, EntityManager entityManager) {
        super(entityInformation, entityManager);
        this.em = entityManager;
        this.domainType = entityInformation.getJavaType();
    }


    @Override
    public Items<T> list(ListParam<T> listParam) {
        return ListParamJpaUtil.list(this.em, listParam, this.domainType);
    }

}