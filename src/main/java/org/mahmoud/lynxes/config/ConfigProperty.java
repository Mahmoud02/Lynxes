package org.mahmoud.lynxes.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark configuration properties that should be loaded from Typesafe Config.
 * Similar to Spring Boot's @Value annotation but for Typesafe Config.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ConfigProperty {
    /**
     * The configuration key path (e.g., "lynxes.server.port")
     */
    String value();
    
    /**
     * Default value if the configuration key is not found
     */
    String defaultValue() default "";
}
