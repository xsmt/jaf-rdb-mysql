package cn.jcloud.jaf.rdb.tenant.reporisoty.impl;

import cn.jcloud.jaf.common.constant.CommonCacheNames;
import cn.jcloud.jaf.common.tenant.domain.Tenant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.jpa.repository.support.JpaMetamodelEntityInformation;
import org.springframework.data.jpa.repository.support.SimpleJpaRepository;
import org.springframework.stereotype.Repository;

import javax.persistence.EntityManager;

/**
 * Created by Wei Han on 2016/1/27.
 */
@Repository
@CacheConfig(cacheNames = CommonCacheNames.TENANT)
public class TenantRepositoryImpl extends SimpleJpaRepository<Tenant, Long> {

    @Autowired
    public TenantRepositoryImpl(EntityManager entityManager) {
        super(JpaMetamodelEntityInformation.getEntityInformation(Tenant.class, entityManager), entityManager);
    }

    @Cacheable
    @Override
    public Tenant findOne(Long id) {
        return super.findOne(id);
    }

    @CacheEvict(key = "#p0.id")
    @Override
    public void delete(Tenant entity) {
        super.delete(entity);
    }

    @CacheEvict(key = "#p0.id")
    @Override
    public Tenant save(Tenant entity) {
        return super.save(entity);
    }
}
