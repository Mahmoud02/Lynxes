package org.mahmoud.fastqueue.server.swagger;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Component scanner that automatically discovers servlet classes with Swagger annotations.
 * Similar to Spring Boot's @ComponentScan but specifically for servlet discovery.
 */
public class ComponentScanner {
    
    private final Set<String> basePackages;
    private final Set<Class<?>> discoveredClasses;
    
    public ComponentScanner(String... basePackages) {
        this.basePackages = new HashSet<>();
        for (String basePackage : basePackages) {
            this.basePackages.add(basePackage);
        }
        this.discoveredClasses = new HashSet<>();
    }
    
    /**
     * Scans the specified packages for servlet classes.
     */
    public Set<Class<?>> scanForServlets() {
        discoveredClasses.clear();
        
        for (String basePackage : basePackages) {
            scanPackage(basePackage);
        }
        
        return new HashSet<>(discoveredClasses);
    }
    
    /**
     * Scans a specific package for servlet classes.
     */
    private void scanPackage(String packageName) {
        try {
            String packagePath = packageName.replace('.', '/');
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            Enumeration<URL> resources = classLoader.getResources(packagePath);
            
            while (resources.hasMoreElements()) {
                URL resource = resources.nextElement();
                if (resource.getProtocol().equals("file")) {
                    scanDirectory(new File(resource.getFile()), packageName);
                } else if (resource.getProtocol().equals("jar")) {
                    scanJarFile(resource, packageName);
                }
            }
        } catch (IOException e) {
            System.err.println("Error scanning package " + packageName + ": " + e.getMessage());
        }
    }
    
    /**
     * Scans a directory for servlet classes.
     */
    private void scanDirectory(File directory, String packageName) {
        if (!directory.exists()) {
            return;
        }
        
        File[] files = directory.listFiles();
        if (files == null) {
            return;
        }
        
        for (File file : files) {
            if (file.isDirectory()) {
                scanDirectory(file, packageName + "." + file.getName());
            } else if (file.getName().endsWith(".class")) {
                String className = packageName + "." + file.getName().substring(0, file.getName().length() - 6);
                loadAndCheckClass(className);
            }
        }
    }
    
    /**
     * Scans a JAR file for servlet classes.
     */
    private void scanJarFile(URL jarUrl, String packageName) {
        try {
            String jarPath = jarUrl.getPath().substring(5, jarUrl.getPath().indexOf("!"));
            JarFile jarFile = new JarFile(jarPath);
            Enumeration<JarEntry> entries = jarFile.entries();
            
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String entryName = entry.getName();
                
                if (entryName.endsWith(".class") && entryName.startsWith(packageName.replace('.', '/'))) {
                    String className = entryName.replace('/', '.').substring(0, entryName.length() - 6);
                    loadAndCheckClass(className);
                }
            }
            
            jarFile.close();
        } catch (IOException e) {
            System.err.println("Error scanning JAR file: " + e.getMessage());
        }
    }
    
    /**
     * Loads a class and checks if it's a servlet with Swagger annotations.
     */
    private void loadAndCheckClass(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            
            // Check if it's a servlet class
            if (isServletClass(clazz)) {
                discoveredClasses.add(clazz);
                System.out.println("Discovered servlet: " + className);
            }
        } catch (ClassNotFoundException e) {
            System.err.println("Could not load class " + className + ": " + e.getMessage());
        }
    }
    
    /**
     * Checks if a class is a servlet class.
     */
    private boolean isServletClass(Class<?> clazz) {
        // Check if it extends HttpServlet
        if (!jakarta.servlet.http.HttpServlet.class.isAssignableFrom(clazz)) {
            return false;
        }
        
        // Check if it's not an abstract class
        if (java.lang.reflect.Modifier.isAbstract(clazz.getModifiers())) {
            return false;
        }
        
        // Check if it's not an interface
        if (clazz.isInterface()) {
            return false;
        }
        
        // Check if it has HTTP methods (doGet, doPost, etc.)
        return hasHttpMethods(clazz);
    }
    
    /**
     * Checks if a class has HTTP methods.
     */
    private boolean hasHttpMethods(Class<?> clazz) {
        String[] httpMethods = {"doGet", "doPost", "doPut", "doDelete", "doPatch"};
        
        for (String methodName : httpMethods) {
            try {
                clazz.getDeclaredMethod(methodName, 
                    jakarta.servlet.http.HttpServletRequest.class, 
                    jakarta.servlet.http.HttpServletResponse.class);
                return true;
            } catch (NoSuchMethodException e) {
                // Method not found, continue checking
            }
        }
        
        return false;
    }
    
    /**
     * Gets all discovered servlet classes.
     */
    public Set<Class<?>> getDiscoveredClasses() {
        return new HashSet<>(discoveredClasses);
    }
    
    /**
     * Gets servlet classes with Swagger annotations.
     */
    public Set<Class<?>> getServletsWithSwaggerAnnotations() {
        Set<Class<?>> swaggerServlets = new HashSet<>();
        
        for (Class<?> servletClass : discoveredClasses) {
            if (hasSwaggerAnnotations(servletClass)) {
                swaggerServlets.add(servletClass);
            }
        }
        
        return swaggerServlets;
    }
    
    /**
     * Checks if a servlet class has Swagger annotations.
     */
    private boolean hasSwaggerAnnotations(Class<?> servletClass) {
        // Check for class-level Swagger annotations
        if (servletClass.isAnnotationPresent(io.swagger.v3.oas.annotations.tags.Tag.class)) {
            return true;
        }
        
        // Check for method-level Swagger annotations
        for (java.lang.reflect.Method method : servletClass.getDeclaredMethods()) {
            if (method.isAnnotationPresent(io.swagger.v3.oas.annotations.Operation.class) ||
                method.isAnnotationPresent(io.swagger.v3.oas.annotations.responses.ApiResponses.class)) {
                return true;
            }
        }
        
        return false;
    }
}
