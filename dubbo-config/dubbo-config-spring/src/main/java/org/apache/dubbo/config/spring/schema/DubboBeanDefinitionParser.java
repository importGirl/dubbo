/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.config.spring.schema;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.AbstractServiceConfig;
import org.apache.dubbo.config.ArgumentConfig;
import org.apache.dubbo.config.ConsumerConfig;
import org.apache.dubbo.config.MethodConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.ProviderConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.spring.ReferenceBean;
import org.apache.dubbo.config.spring.ServiceBean;
import org.apache.dubbo.config.spring.beans.factory.annotation.DubboConfigAliasPostProcessor;

import org.springframework.beans.PropertyValue;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.config.TypedStringValue;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static com.alibaba.spring.util.BeanRegistrar.registerInfrastructureBean;
import static org.apache.dubbo.common.constants.CommonConstants.HIDE_KEY_PREFIX;

/**
 * AbstractBeanDefinitionParser
 *
 * @export
 */
public class DubboBeanDefinitionParser implements BeanDefinitionParser {

    private static final Logger logger = LoggerFactory.getLogger(DubboBeanDefinitionParser.class);
    private static final Pattern GROUP_AND_VERSION = Pattern.compile("^[\\-.0-9_a-zA-Z]+(\\:[\\-.0-9_a-zA-Z]+)?$");
    private static final String ONRETURN = "onreturn";
    private static final String ONTHROW = "onthrow";
    private static final String ONINVOKE = "oninvoke";
    private static final String METHOD = "Method";
    private final Class<?> beanClass;   // Bean对象的类
    private final boolean required;     // 是否需要Bean的id属性

    public DubboBeanDefinitionParser(Class<?> beanClass, boolean required) {
        this.beanClass = beanClass;
        this.required = required;
    }

    /**
     * 解析为 BeanDefinition ； 交给spring管理
     * - id属性查找顺序: id -> name -> protocol默认dubbo-> interface ->beanClassName
     * - RuntimeBeanReference； 可动态注册的bean对象
     * @param element
     * @param parserContext
     * @param beanClass
     * @param required
     * @return
     */
    @SuppressWarnings("unchecked")
    private static BeanDefinition parse(Element element, ParserContext parserContext, Class<?> beanClass, boolean required) {
        // 创建 beanDefinition 对象
        RootBeanDefinition beanDefinition = new RootBeanDefinition();
        beanDefinition.setBeanClass(beanClass);
        beanDefinition.setLazyInit(false);
        // 【标签】id属性
        String id = element.getAttribute("id");
        // id为空
        if (StringUtils.isEmpty(id) && required) {
            // 【标签】name属性
            String generatedBeanName = element.getAttribute("name");
            // name为空
            if (StringUtils.isEmpty(generatedBeanName)) {
                // protoColConfig 默认为dubbo
                if (ProtocolConfig.class.equals(beanClass)) {
                    generatedBeanName = "dubbo";
                }
                //【标签】 interface属性
                else {
                    generatedBeanName = element.getAttribute("interface");
                }
            }
            // 实在不行使用类名
            if (StringUtils.isEmpty(generatedBeanName)) {
                generatedBeanName = beanClass.getName();
            }
            // 赋值给id
            id = generatedBeanName;
            int counter = 2;
            // [Duplicate策略]该bean是否重复注册了； example: userService3
            while (parserContext.getRegistry().containsBeanDefinition(id)) {
                id = generatedBeanName + (counter++);
            }
        }
        if (StringUtils.isNotEmpty(id)) {
            // 检查 userService3 仍然注册，就抛异常吧！
            if (parserContext.getRegistry().containsBeanDefinition(id)) {
                throw new IllegalStateException("Duplicate spring bean id " + id);
            }
            // 注册beanDefinition; id=beanDefinition
            parserContext.getRegistry().registerBeanDefinition(id, beanDefinition);
            // 设置Config的 'id' 属性； （具体实现逻辑要看spring了)
            beanDefinition.getPropertyValues().addPropertyValue("id", id);
        }
        // 【特殊】protocolConfig
        if (ProtocolConfig.class.equals(beanClass)) {
            // 遍历注册的BeanDefinitionNames
            for (String name : parserContext.getRegistry().getBeanDefinitionNames()) {
                // beanDefinition
                BeanDefinition definition = parserContext.getRegistry().getBeanDefinition(name);
                // 获取protocol属性值
                PropertyValue property = definition.getPropertyValues().getPropertyValue("protocol");
                // 重新设置 protocol属性的值
                if (property != null) {
                    Object value = property.getValue();
                    if (value instanceof ProtocolConfig && id.equals(((ProtocolConfig) value).getName())) {
                        definition.getPropertyValues().addPropertyValue("protocol", new RuntimeBeanReference(id));
                    }
                }
            }
        }
        // 【特殊】 serviceBean
        else if (ServiceBean.class.equals(beanClass)) {
            //【标签】 class 属性
            String className = element.getAttribute("class");
            if (StringUtils.isNotEmpty(className)) {
                // 使用className 创建 beanDefinition
                RootBeanDefinition classDefinition = new RootBeanDefinition();
                classDefinition.setBeanClass(ReflectUtils.forName(className));
                classDefinition.setLazyInit(false);
                // 【标签】子标签 <property/> 列表， 并赋值给 classDefinition
                parseProperties(element.getChildNodes(), classDefinition);
                // 设置config 的ref属性值
                beanDefinition.getPropertyValues().addPropertyValue("ref", new BeanDefinitionHolder(classDefinition, id + "Impl"));
            }
        }
        // 【特殊】providerConfig; 解析子标签：<dubbo:service/>
        else if (ProviderConfig.class.equals(beanClass)) {
            // <dubbo:provider><dubbo:service/></dubbo:provider>; 设置service类的provider属性值
            parseNested(element, parserContext, ServiceBean.class, true, "service", "provider", id, beanDefinition);
        }
        // 【特殊】consumerConfig; 解析子标签：<dubbo:reference/>
        else if (ConsumerConfig.class.equals(beanClass)) {
            //  <dubbo:consumer><dubbo:reference/></dubbo:consumer>; 设置 reference 类的 consumer值
            parseNested(element, parserContext, ReferenceBean.class, false, "reference", "consumer", id, beanDefinition);
        }
        Set<String> props = new HashSet<>();
        ManagedMap parameters = null;
        // 遍历 setter methods; 将属性赋值到到config对象上
        for (Method setter : beanClass.getMethods()) {
            String name = setter.getName();
            // 判断为 public void setUserId(Long id){} 方法
            if (name.length() > 3 && name.startsWith("set")
                    && Modifier.isPublic(setter.getModifiers())
                    && setter.getParameterTypes().length == 1) {
                // 参数列表类型
                Class<?> type = setter.getParameterTypes()[0];
                // 获得 [userId]
                String beanProperty = name.substring(3, 4).toLowerCase() + name.substring(4);
                // 返回 user-id
                String property = StringUtils.camelToSplitName(beanProperty, "-");
                // 添加 user-id
                props.add(property);
                // check the setter/getter whether match
                Method getter = null;
                try {
                    // 返回 getUserId方法
                    getter = beanClass.getMethod("get" + name.substring(3), new Class<?>[0]);
                } catch (NoSuchMethodException e) {
                    try {
                        // 无则返回 isUserId方法
                        getter = beanClass.getMethod("is" + name.substring(3), new Class<?>[0]);
                    } catch (NoSuchMethodException e2) {
                        // ignore, there is no need any log here since some class implement the interface: EnvironmentAware,
                        // ApplicationAware, etc. They only have setter method, otherwise will cause the error log during application start up.
                    }
                }
                // 判断是 public Long getUserId(){}
                if (getter == null
                        || !Modifier.isPublic(getter.getModifiers())
                        || !type.equals(getter.getReturnType())) {
                    continue;
                }
                // beanClass 中有 parameters属性； private Map<String,Object> parameters;
                if ("parameters".equals(property)) {
                    // 【子标签】<dubbo:property/>列表
                    parameters = parseParameters(element.getChildNodes(), beanDefinition);
                }
                // beanClass 中有 methods 属性；private List<Method> methods;
                else if ("methods".equals(property)) {
                    // 【子标签】<dubbo:methods/>列表
                    parseMethods(id, element.getChildNodes(), beanDefinition, parserContext);
                }
                // beanClass 中有 arguments 属性； private List<Argument> arguments;
                else if ("arguments".equals(property)) {
                    // 【子标签】 <dubbo:argument index="xxx"/> 列表
                    parseArguments(id, element.getChildNodes(), beanDefinition, parserContext);
                } else {
                    // 【标签】 对应属性的值
                    String value = element.getAttribute(property);
                    if (value != null) {
                        value = value.trim();
                        if (value.length() > 0) {
                            // [不想注册到注册中心的情况] registry = "N/A";
                            if ("registry".equals(property) && RegistryConfig.NO_AVAILABLE.equalsIgnoreCase(value)) {
                                RegistryConfig registryConfig = new RegistryConfig();
                                registryConfig.setAddress(RegistryConfig.NO_AVAILABLE);
                                beanDefinition.getPropertyValues().addPropertyValue(beanProperty, registryConfig);
                            }
                            // [多注册中心的情况]、[多提供者情况]、[多协议情况]
                            else if ("provider".equals(property) || "registry".equals(property) || ("protocol".equals(property) && AbstractServiceConfig.class.isAssignableFrom(beanClass))) {
                                /**
                                 * For 'provider' 'protocol' 'registry', keep literal value (should be id/name) and set the value to 'registryIds' 'providerIds' protocolIds'
                                 * The following process should make sure each id refers to the corresponding instance, here's how to find the instance for different use cases:
                                 * 1. Spring, check existing bean by id, see{@link ServiceBean#afterPropertiesSet()}; then try to use id to find configs defined in remote Config Center
                                 * 2. API, directly use id to find configs defined in remote Config Center; if all config instances are defined locally, please use {@link ServiceConfig#setRegistries(List)}
                                 */
                                // 设置 registryIds,providerIds属性
                                beanDefinition.getPropertyValues().addPropertyValue(beanProperty + "Ids", value);
                            } else {
                                Object reference;
                                // 基本类型
                                if (isPrimitive(type)) {
                                    if ("async".equals(property) && "false".equals(value)
                                            || "timeout".equals(property) && "0".equals(value)
                                            || "delay".equals(property) && "0".equals(value)
                                            || "version".equals(property) && "0.0.0".equals(value)
                                            || "stat".equals(property) && "-1".equals(value)
                                            || "reliable".equals(property) && "false".equals(value)) {
                                        // backward compatibility for the default value in old version's xsd
                                        value = null;
                                    }
                                    reference = value;
                                }
                                // 异步调用相关方法
                                else if (ONRETURN.equals(property) || ONTHROW.equals(property) || ONINVOKE.equals(property)) {
                                    int index = value.lastIndexOf(".");
                                    String ref = value.substring(0, index);
                                    String method = value.substring(index + 1);
                                    reference = new RuntimeBeanReference(ref);
                                    beanDefinition.getPropertyValues().addPropertyValue(property + METHOD, method);
                                } else {
                                    // 【标签】 ref属性值， 是否已经加载注册
                                    if ("ref".equals(property) && parserContext.getRegistry().containsBeanDefinition(value)) {
                                        BeanDefinition refBean = parserContext.getRegistry().getBeanDefinition(value);
                                        // 是否单例
                                        if (!refBean.isSingleton()) {
                                            throw new IllegalStateException("The exported service ref " + value + " must be singleton! Please set the " + value + " bean scope to singleton, eg: <bean id=\"" + value + "\" scope=\"singleton\" ...>");
                                        }
                                    }
                                    reference = new RuntimeBeanReference(value);
                                }
                                // 设置标签 ref对应的值
                                beanDefinition.getPropertyValues().addPropertyValue(beanProperty, reference);
                            }
                        }
                    }
                }
            }
        }
        // 【标签】 属性； 设置 beanDefinition 的 parameters属性值
        NamedNodeMap attributes = element.getAttributes();
        int len = attributes.getLength();
        for (int i = 0; i < len; i++) {
            Node node = attributes.item(i);
            String name = node.getLocalName();
            if (!props.contains(name)) {
                if (parameters == null) {
                    parameters = new ManagedMap();
                }
                String value = node.getNodeValue();
                parameters.put(name, new TypedStringValue(value, String.class));
            }
        }
        if (parameters != null) {
            beanDefinition.getPropertyValues().addPropertyValue("parameters", parameters);
        }

        // 返回
        return beanDefinition;
    }

    private static boolean isPrimitive(Class<?> cls) {
        return cls.isPrimitive() || cls == Boolean.class || cls == Byte.class
                || cls == Character.class || cls == Short.class || cls == Integer.class
                || cls == Long.class || cls == Float.class || cls == Double.class
                || cls == String.class || cls == Date.class || cls == Class.class;
    }

    /**
     * 解析 element 元素下的对应 tag 的子标签列表；并设置子对象（beandefinition) 的 property属性;
     * - <dubbo:provider><dubbo:service/></dubbo:provider>; 设置service类的provider属性值
     * - <dubbo:consumer><dubbo:reference/></dubbo:consumer>; 设置 reference 类的 consumer值
     * @param element 标签 <dubbo:provider/> <dubbo:consumer/>
     * @param parserContext
     * @param beanClass
     * @param required
     * @param tag       example: service/reference
     * @param property  example: provider/consumer
     * @param ref
     * @param beanDefinition
     */
    private static void parseNested(Element element, ParserContext parserContext, Class<?> beanClass, boolean required, String tag, String property, String ref, BeanDefinition beanDefinition) {
        // 【子标签】 example: <dubbo:service/>、<dubbo:reference/>
        NodeList nodeList = element.getChildNodes();
        if (nodeList == null) {
            return;
        }
        boolean first = true;
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);
            if (!(node instanceof Element)) {
                continue;
            }
            // example: tag = service、reference
            if (tag.equals(node.getNodeName())
                    || tag.equals(node.getLocalName())) {
                // 设置 defalult 属性值
                if (first) {
                    first = false;
                    String isDefault = element.getAttribute("default");
                    if (StringUtils.isEmpty(isDefault)) {
                        beanDefinition.getPropertyValues().addPropertyValue("default", "false");
                    }
                }
                // 【递归】递归加载 node标签
                BeanDefinition subDefinition = parse((Element) node, parserContext, beanClass, required);
                // 设置 property(提供者/消费者） 值
                if (subDefinition != null && StringUtils.isNotEmpty(ref)) {
                    subDefinition.getPropertyValues().addPropertyValue(property, new RuntimeBeanReference(ref));
                }
            }
        }
    }

    /**
     * 解析 <property /> 标签列表， 并赋值给beanDefinition
     * @param nodeList
     * @param beanDefinition
     */
    private static void parseProperties(NodeList nodeList, RootBeanDefinition beanDefinition) {
        if (nodeList == null) {
            return;
        }
        for (int i = 0; i < nodeList.getLength(); i++) {
            if (!(nodeList.item(i) instanceof Element)) {
                continue;
            }
            Element element = (Element) nodeList.item(i);
            // 【标签】 <property name="xxx" value="xxx" ref="com.xxx.xx.userService"/>标签
            if ("property".equals(element.getNodeName())
                    || "property".equals(element.getLocalName())) {
                // 【标签】name属性
                String name = element.getAttribute("name");
                if (StringUtils.isNotEmpty(name)) {
                    // 【标签】value,ref属性
                    String value = element.getAttribute("value");
                    String ref = element.getAttribute("ref");
                    // beanDefinition 设置 'name' 的属性值
                    if (StringUtils.isNotEmpty(value)) {
                        beanDefinition.getPropertyValues().addPropertyValue(name, value);
                    } else if (StringUtils.isNotEmpty(ref)) {
                        beanDefinition.getPropertyValues().addPropertyValue(name, new RuntimeBeanReference(ref));
                    } else {
                        throw new UnsupportedOperationException("Unsupported <property name=\"" + name + "\"> sub tag, Only supported <property name=\"" + name + "\" ref=\"...\" /> or <property name=\"" + name + "\" value=\"...\" />");
                    }
                }
            }
        }
    }

    /**
     * 解析 <dubbo:property key="xxx" value="xxx" hide="true"/> 标签； 并返回 Map
     * @param nodeList
     * @param beanDefinition
     * @return
     */
    @SuppressWarnings("unchecked")
    private static ManagedMap parseParameters(NodeList nodeList, RootBeanDefinition beanDefinition) {
        if (nodeList == null) {
            return null;
        }
        ManagedMap parameters = null;
        for (int i = 0; i < nodeList.getLength(); i++) {
            if (!(nodeList.item(i) instanceof Element)) {
                continue;
            }
            Element element = (Element) nodeList.item(i);
            // 【标签】<dubbo:parameter/>
            if ("parameter".equals(element.getNodeName())
                    || "parameter".equals(element.getLocalName())) {
                if (parameters == null) {
                    parameters = new ManagedMap();
                }
                // 【标签】key,value,hide属性
                String key = element.getAttribute("key");
                String value = element.getAttribute("value");
                boolean hide = "true".equals(element.getAttribute("hide"));
                if (hide) {
                    key = HIDE_KEY_PREFIX + key;
                }
                // 设置到Map中
                parameters.put(key, new TypedStringValue(value, String.class));
            }
        }
        return parameters;
    }

    /**
     * 解析 <dubbo:methods>
     *       <dubbo:parameter/>
     *       <dubbo:argument/>
     *     </dubbo:methods>
     * 标签； 并设置 beanDefinition 的methods属性
     * @param id
     * @param nodeList
     * @param beanDefinition
     * @param parserContext
     */
    @SuppressWarnings("unchecked")
    private static void parseMethods(String id, NodeList nodeList, RootBeanDefinition beanDefinition,
                                     ParserContext parserContext) {
        if (nodeList == null) {
            return;
        }
        ManagedList methods = null;
        for (int i = 0; i < nodeList.getLength(); i++) {
            if (!(nodeList.item(i) instanceof Element)) {
                continue;
            }
            Element element = (Element) nodeList.item(i);
            // 【标签】<dubbo:methods name="xxx" />
            if ("method".equals(element.getNodeName()) || "method".equals(element.getLocalName())) {
                String methodName = element.getAttribute("name");
                if (StringUtils.isEmpty(methodName)) {
                    throw new IllegalStateException("<dubbo:method> name attribute == null");
                }
                if (methods == null) {
                    methods = new ManagedList();
                }
                // 【递归】method的子标签： <dubbo:parameter/> , <dubbo:argument/>
                BeanDefinition methodBeanDefinition = parse(element,
                        parserContext, MethodConfig.class, false);
                // 方法名
                String name = id + "." + methodName;
                // beandefinition
                BeanDefinitionHolder methodBeanDefinitionHolder = new BeanDefinitionHolder(
                        methodBeanDefinition, name);
                methods.add(methodBeanDefinitionHolder);
            }
        }
        // 设置 methods属性
        if (methods != null) {
            beanDefinition.getPropertyValues().addPropertyValue("methods", methods);
        }
    }

    /**
     * 解析 <dubbo:argument index="xxx"/> 属性；
     * 并设置到beanDefiniton 的arguments属性
     * @param id
     * @param nodeList
     * @param beanDefinition
     * @param parserContext
     */
    @SuppressWarnings("unchecked")
    private static void parseArguments(String id, NodeList nodeList, RootBeanDefinition beanDefinition,
                                       ParserContext parserContext) {
        if (nodeList == null) {
            return;
        }
        ManagedList arguments = null;
        for (int i = 0; i < nodeList.getLength(); i++) {
            if (!(nodeList.item(i) instanceof Element)) {
                continue;
            }
            Element element = (Element) nodeList.item(i);
            // 【标签】<dubbo:argument index="xxx" />
            if ("argument".equals(element.getNodeName()) || "argument".equals(element.getLocalName())) {
                String argumentIndex = element.getAttribute("index");
                if (arguments == null) {
                    arguments = new ManagedList();
                }
                // 【递归】 解析 argument标签， 它已经没有子标签了
                BeanDefinition argumentBeanDefinition = parse(element,
                        parserContext, ArgumentConfig.class, false);
                String name = id + "." + argumentIndex;
                BeanDefinitionHolder argumentBeanDefinitionHolder = new BeanDefinitionHolder(
                        argumentBeanDefinition, name);
                arguments.add(argumentBeanDefinitionHolder);
            }
        }
        // 设置 arguments属性
        if (arguments != null) {
            beanDefinition.getPropertyValues().addPropertyValue("arguments", arguments);
        }
    }

    /**
     * spring 解析 xml时候调用
     * @param element
     * @param parserContext
     * @return
     */
    @Override
    public BeanDefinition parse(Element element, ParserContext parserContext) {
        // Register DubboConfigAliasPostProcessor
        registerDubboConfigAliasPostProcessor(parserContext.getRegistry());

        // 解析xml标签
        return parse(element, parserContext, beanClass, required);
    }

    /**
     * Register {@link DubboConfigAliasPostProcessor}
     * - RootBeanDefinition beanDefinition = new RootBeanDefinition(beanType);
     *
     * @param registry {@link BeanDefinitionRegistry}
     * @since 2.7.5 [Feature] https://github.com/apache/dubbo/issues/5093
     */
    private void registerDubboConfigAliasPostProcessor(BeanDefinitionRegistry registry) {
        registerInfrastructureBean(registry, DubboConfigAliasPostProcessor.BEAN_NAME, DubboConfigAliasPostProcessor.class);
    }
}
