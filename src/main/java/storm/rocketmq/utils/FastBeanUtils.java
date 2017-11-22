package storm.rocketmq.utils;

import com.google.common.base.Preconditions;
import net.sf.cglib.beans.BeanCopier;
import net.sf.cglib.core.Converter;
import net.sf.cglib.core.ReflectUtils;

/**
 * Created by zhangfengguang on 2017/11/21.
 */
public abstract class FastBeanUtils {
	public static Object copyProperties(Object source, Class<?> target) {
		Preconditions.checkNotNull(source, "Source must not be null!");
		Preconditions.checkNotNull(target, "Target must not be null!");

		Object targetObject = ReflectUtils.newInstance(target);
		BeanCopier beanCopier = BeanCopier.create(source.getClass(), target, false);
		beanCopier.copy(source, targetObject, null);

		return targetObject;
	}

	public static Object copyProperties(Object source, Class<?> target, Converter converter) {
		Preconditions.checkNotNull(source, "Source must not be null");
		Preconditions.checkNotNull(target, "Target must not be null");

		Object targetObject = ReflectUtils.newInstance(target);
		BeanCopier beanCopier = BeanCopier.create(source.getClass(), target, true);
		beanCopier.copy(source, targetObject, converter);

		return targetObject;
	}

	private FastBeanUtils() {
	}
}
