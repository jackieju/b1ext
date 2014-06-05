package com.sap.sbo.bofrw.jaw.bo;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;

import com.sap.sbo.bofrw.common.PermissionLevel;
import com.sap.sbo.bofrw.common.bo.BORepository;
import com.sap.sbo.bofrw.common.bo.BusinessObject;
import com.sap.sbo.bofrw.common.bo.BusinessObjectExitHandler;
import com.sap.sbo.bofrw.common.bo.BusinessObjectFacade;
import com.sap.sbo.bofrw.common.bo.BusinessObjectIdentifier;
import com.sap.sbo.bofrw.common.bo.BusinessObjectNode;
import com.sap.sbo.bofrw.common.bo.BusinessObjectNodeList;
import com.sap.sbo.bofrw.common.bo.OnBOChangeParam;
import com.sap.sbo.bofrw.common.bo.UserFields;
import com.sap.sbo.bofrw.common.bo.annotation.BOImplementation;
import com.sap.sbo.bofrw.common.bo.permission.BOPermissionChecker;
import com.sap.sbo.bofrw.common.bo.query.Constant;
import com.sap.sbo.bofrw.common.bo.query.Criteria;
import com.sap.sbo.bofrw.common.bo.query.Operator.And;
import com.sap.sbo.bofrw.common.bo.query.Operator.Equal;
import com.sap.sbo.bofrw.common.bo.query.Operator.NotEqual;
import com.sap.sbo.bofrw.common.bo.query.Path;
import com.sap.sbo.bofrw.common.bo.query.Predicate;
import com.sap.sbo.bofrw.common.bo.validation.ValidationRepository;
import com.sap.sbo.bofrw.common.cache.util.ClassHelper;
import com.sap.sbo.bofrw.common.meta.ActionDynamicMeta;
import com.sap.sbo.bofrw.common.meta.BusinessObjectDynamicMeta;
import com.sap.sbo.bofrw.common.meta.PropertyDynamicMeta;
import com.sap.sbo.bofrw.common.meta.SubNodeDynamicMeta;
import com.sap.sbo.bofrw.common.meta.model.dynamic.BusinessObjectDynamicMetaImpl;
import com.sap.sbo.bofrw.common.meta.model.gdt.EnumTypeUtils;
import com.sap.sbo.bofrw.jaw.ObserverBusinessObjectFacade;
import com.sap.sbo.bofrw.jaw.action.DefaultBOActionExecutor;
import com.sap.sbo.bofrw.jaw.action.DefaultOnBOChangeExecutor;
import com.sap.sbo.bofrw.jaw.observer.InteropContext;
import com.sap.sbo.bofrw.jaw.observer.OBServerException;
import com.sap.sbo.bofrw.jaw.observer.ProxyService;
import com.sap.sbo.bofrw.jaw.observer.ProxyServiceData;
import com.sap.sbo.bofrw.jaw.observer.Releaseable;
import com.sap.sbo.bofrw.jaw.type.Recordset;
import com.sap.sbo.bofrw.metadata.Action;
import com.sap.sbo.bofrw.metadata.Association;
import com.sap.sbo.bofrw.metadata.BOBaseType;
import com.sap.sbo.bofrw.metadata.BOSimpleType;
import com.sap.sbo.bofrw.metadata.BusinessObjectType;
import com.sap.sbo.bofrw.metadata.ComplexType;
import com.sap.sbo.bofrw.metadata.InputParameter;
import com.sap.sbo.bofrw.metadata.Node;
import com.sap.sbo.bofrw.metadata.NodeType;
import com.sap.sbo.bofrw.metadata.OnBOChange;
import com.sap.sbo.bofrw.metadata.OutputParameter;
import com.sap.sbo.bofrw.metadata.Property;
import com.sap.sbo.bofrw.metadata.PropertyRef;
import com.sap.sbo.bofrw.metadata.repository.TypeUtils;
import com.sap.sbo.common.errorcode.BOFrwJawErrorCode;
import com.sap.sbo.common.exception.BaseException;
import com.sap.sbo.common.exception.BusinessException;
import com.sap.sbo.common.exception.LocatableBusinessException;
import com.sap.sbo.common.exception.SystemException;
import com.sap.sbo.common.log.Logger;
import com.sap.sbo.common.log.LoggerFactory;

// for ruby
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.ScriptContext;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import org.jruby.embed.LocalVariableBehavior;
import org.jruby.embed.ScriptingContainer;

public class ObserverBusinessObject implements BusinessObject, Releaseable {
// for jruby	
	public static ScriptEngine jruby = new ScriptEngineManager().getEngineByName("jruby");
    public static String ruby_root = "/var/sa/ruby";
    public static String ruby_ext_root = "/var/sa/ruby/extroot";
	static {
 	try{
                System.out.println("Starting ruby engine ..."+jruby.getClass());
String separator = System.getProperty("path.separator");
String classpath = System.getProperty("java.class.path");
classpath = classpath + separator + ruby_root;
//System.setProperty("java.class.path", classpath);
System.setProperty("com.sun.script.jruby.loadpath", ruby_root);
                jruby.eval(new BufferedReader(new FileReader(ruby_root+"/main.rb")));
                System.out.println("Start ruby engine OK.");
        }catch(Throwable e){
                System.out.println("WARN: run ruby script main.rb failed:");
                e.printStackTrace();
        }
	}
    public static void loadRuby(){
        try{
                System.out.println("Starting ruby engine ..."+jruby.getClass());
String separator = System.getProperty("path.separator");
String classpath = System.getProperty("java.class.path");
classpath = classpath + separator + ruby_root;
//System.setProperty("java.class.path", classpath);
System.setProperty("com.sun.script.jruby.loadpath", ruby_root);
                jruby.eval(new BufferedReader(new FileReader(ruby_root+"/main.rb")));
                System.out.println("Start ruby engine OK.");
        }catch(Throwable e){
                System.out.println("WARN: run ruby script main.rb failed:");
                e.printStackTrace();
        }
    }

    private static Logger LOGGER = LoggerFactory.getLogger(ObserverBusinessObject.class);

    private static String RECORDSET = "Recordset";

    private final BusinessObjectDynamicMeta dynamicMeta;

    private final BusinessObjectType boMeta;

    private final ProxyService proxyService;

    protected ObserverBusinessObjectNode rootNode;

    private boolean isReleased = false;

    protected BusinessObjectIdentifier boIdentifier;

    protected BusinessObjectIdentifier bkIdentifier;

    protected ObserverBusinessObjectParamProxy boDynamicMetas;

    protected BusinessObjectFacade boFacade;

    private InteropContext context;

    private boolean byPassAutoComplete = false;

    private static final long REFRESH_METADATA_COMMAND_CODE = 81081;

    private final Collection<BusinessObjectExitHandler> exitHandlers = new HashSet<BusinessObjectExitHandler>();

    protected void setContext(InteropContext context) {
        this.context = context;
    }

    protected InteropContext getContext() {
        return this.context;
    }

    protected ObserverBusinessObject(ObserverBusinessObject bo) {
        this.dynamicMeta = bo.dynamicMeta;
        this.boMeta = bo.boMeta;
        this.proxyService = bo.proxyService;
    }

    protected ObserverBusinessObject(ProxyService proxyService, ObserverBusinessObjectFacade boFacade) {
        if (proxyService == null)
            throw new IllegalArgumentException("ProxyService");

        if (null == boFacade)
            throw new IllegalArgumentException("boFacade");

        this.proxyService = proxyService;
        this.boFacade = boFacade;
        BOImplementation boImplementation = this.getPureJavaType().getAnnotation(BOImplementation.class);
        boMeta = boFacade.getMetadataRepository().getBusinessObjectTypeByCode(boImplementation.code());
        dynamicMeta = new BusinessObjectDynamicMetaImpl(boMeta);
        rootNode = new ObserverBusinessObjectNode(this, null);
    }

    public BusinessObjectType getBoMeta() {
        return this.boMeta;
    }

    public <T extends BusinessObject> BORepository<T> getBORepository() {
        BORepository<T> boRepository = boFacade.getBORepository(this.boMeta.getNamespace(), this.boMeta.getName());
        return boRepository;
    }

    public ProxyService getProxyService() {
        return proxyService;
    }

    /**
     * @return the boFacade
     */
    public BusinessObjectFacade getBoFacade() {
        return boFacade;
    }

    protected ObserverBusinessObjectParamProxy getParameterProxy(String paramName) {
        return new ObserverBusinessObjectParamProxy(this, paramName);
    }

    public void initPropertyWithDefaultValue(List<? extends Property> properties) {
        for (Property property : properties) {
            String defaultValue = property.getDefaultValue();
            if (!StringUtils.isEmpty(defaultValue)) {
                BOBaseType baseType = BOBaseType.fromSimpleType(property.getType());
                if (baseType != null) {
                    Object value = baseType.parse(defaultValue);
                    this.setPropertyValue(property.getName(), value);
                } else {
                    Object value = EnumTypeUtils.valueOf(property.getType(), defaultValue);
                    if (value != null) {
                        this.setPropertyValue(property.getName(), value);
                    }
                }
            } else if (property.isUserDefined() && BOSimpleType.fromValue(property.getType()) == BOSimpleType.TIME) {
            	DateTime now = new DateTime();
            	int currentTime = now.getHourOfDay() * 100 + now.getMinuteOfHour();
            	this.setPropertyValue(property.getName(), currentTime);
            }
        }
    }

    public void refreshDynamicMeatadata(){
        this.doCommand(REFRESH_METADATA_COMMAND_CODE);
        this.processBoDynamicMetas();
    }

    protected void doCommand(long cmdCode) {
    	LOGGER.error("Start to doCommand (" + cmdCode + ")...");
System.out.println("====>doCommand "+ cmdCode +": "+this.getClass());
        try {
            if (cmdCode == 201) {
                String ownerProperty = this.boMeta.getRootNode().getImplOwnerProperty();
                if (ownerProperty != null && !"".equals(ownerProperty) && null != boFacade.getCurrentUser()) {
                    Integer employeeId = boFacade.getCurrentUser().getEmployeeId();
                    if (employeeId != null && this.getPropertyValue(ownerProperty) == null) {
                        // if the owner code is already set, e.g. mass data
                        // import scenario, don't set the ownerCode to current
                        // user
                        this.setPropertyValue(ownerProperty, employeeId);
                    }
                }
                String creatorProperty = this.boMeta.getRootNode().getImplCreatorProperty();
                if (creatorProperty != null && !"".equals(creatorProperty) && null != boFacade.getCurrentUser()) {
                    Integer employeeId = boFacade.getCurrentUser().getEmployeeId();
                    if (employeeId != null) {
                        this.setPropertyValue(creatorProperty, employeeId);
                    }
                }
            }

            if (cmdCode == 201 || cmdCode == 202) {
                validateBoProperties();
                validateUniqueProperties();
            }

            // Bypass auto complete
            if(byPassAutoComplete){
            	this.proxyService.setByPassAutoComplete(true);
            }

            this.proxyService.doCommand(cmdCode);

        //list all available scripting engines
        //listScriptingEngines();
        //get jruby engine
       // ScriptEngine jruby = new ScriptEngineManager().getEngineByName("jruby");
        //process a ruby file
        /*jruby.eval(new BufferedReader(new FileReader("myruby.rb")));

        //call a method defined in the ruby source
        jruby.put("number", 6);
        jruby.put("title", "My Swing App");

        long fact = (Long) jruby.eval("showFactInWindow($title,$number)");
        System.out.println("fact: " + fact);

        jruby.eval("$myglobalvar = fact($number)");
        long myglob = (Long) jruby.getBindings(ScriptContext.ENGINE_SCOPE).get("myglobalvar");
        System.out.println("myglob: " + myglob);
*/	
	try{
        loadRuby();
	//	String ruby_root = "/var/sa/ruby/";
	//	jruby.eval(new BufferedReader(new FileReader(ruby_root+"main.rb")));
        String r_s = "loadObject(\"" + this.getClass().getName()+"\" ,nil, '/var/sa/ruby/root/').after_command('"+cmdCode+"')";
        System.out.println("run ruby script:"+r_s);
        jruby.eval("puts '=>>>hello call ruby ok'");
		jruby.eval(r_s);
		System.out.println("run ruby extension OK.");
	}catch(Throwable e){
		System.out.println("WARN: run ruby script failed:");
		e.printStackTrace();
	}
        } catch (OBServerException e) {
            if (e.getErrorCode() == -2028){
                throw new BusinessException(BOFrwJawErrorCode.DATA_NOT_FOUND,e);
            }
            throw new BusinessException(BOFrwJawErrorCode.OBSERVER_ERROR , e ,e.getMessage());
        }finally{
        	if(byPassAutoComplete){
            	this.proxyService.setByPassAutoComplete(false);
            }
        }
        LOGGER.debug("End to doCommand");
    }

    protected <N extends ObserverBusinessObjectNode> ObserverBusinessObjectNodeList<N> createNodeList(Class<N> clazz) {
        BusinessObjectType businessObjectMetadata = rootNode.getBO().getBoMeta();
        if (null != businessObjectMetadata.getNodeType(clazz.getSimpleName())) {
            return new ObserverBusinessObjectNodeList<N>(this.rootNode, clazz);
        }
        return null;
    }

    @Override
    public <N extends BusinessObjectNode> N createNode(String nodeName) {
        return this.rootNode.createNode(nodeName);
    }

    @Override
    public <N extends BusinessObjectNode> BusinessObjectNodeList<N> getNodeList(String nodeCollectionName) {
        return this.rootNode.getNodeList(nodeCollectionName);
    }

    @Override
    public Object getPropertyValue(String propertyName) {
        Property property = this.boMeta.getRootNode().getProperty(propertyName);
        if (property == null) {
            throw new SystemException(BOFrwJawErrorCode.INVALID_BO_PROPERTY, propertyName);
        } else if (property.getImplColumn() == null || property.getImplColumn().trim().equals("")) {
            String methodName = "get" + StringUtils.capitalize(propertyName);

            Method m;
            try {
                m = this.getClass().getMethod(methodName);
            } catch (NoSuchMethodException | SecurityException e) {
                throw new SystemException(BOFrwJawErrorCode.PROPERTY_NO_GET_METHOD, e,propertyName);
            }
            try {
                return m.invoke(this);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw new SystemException(BOFrwJawErrorCode.METHOD_INVOKE_ERROR, e);
            }
        } else
            return rootNode.getPropertyValue(propertyName);
    }

    @Override
    public void setPropertyValue(String propertyName, Object propertyValue) {
        Property property = this.boMeta.getRootNode().getProperty(propertyName);

        if (property == null) {
            throw new SystemException(BOFrwJawErrorCode.INVALID_BO_PROPERTY, propertyName);
        } else if (property != null && (property.getImplColumn() == null || property.getImplColumn().trim().equals(""))) {
            String methodName = "set" + StringUtils.capitalize(propertyName);
            Method m = null;
            try {
                m = this.getClass().getMethod(methodName,propertyValue.getClass());
            } catch (NoSuchMethodException | SecurityException e) {
                throw new SystemException(BOFrwJawErrorCode.PROPERTY_NO_SET_METHOD, e,propertyName);
            }
            try {
                m.invoke(this, propertyValue);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw new SystemException(BOFrwJawErrorCode.METHOD_INVOKE_ERROR, e);
            }

        } else{
            rootNode.setPropertyValue(propertyName, propertyValue);
        }
    }

    public Object getUserPropertyValue(String udfName) {
    	return rootNode.getUserPropertyValue(udfName);
    }

    public void setUserPropertyValue(String udfName, Object propertyValue) {
    	rootNode.setUserPropertyValue(udfName, propertyValue);
    }

    @Override
    public String getTypeCode() {
        return this.boMeta.getImplCode();
    }

    @Override
    public BusinessObjectDynamicMeta getDynamicMeta() {
        return dynamicMeta;
    }

    @Override
    public UserFields getUserFields() {
        return this.rootNode.getUserFields();
    }

    protected void refreshNodeStructure() {
        this.rootNode.refreshNodeStructure();
    }

    public Integer getCommondCode4GetByKey() {
        return 200;
    }

    public void getByKey() {
        BOPermissionChecker.getChecker().checkReadOnly(boMeta.getNamespace(), boMeta.getName(), boFacade);

        Integer commondCode4GetByKey = getCommondCode4GetByKey();
        this.doCommand(commondCode4GetByKey);

        // refresh node structure
        refreshNodeStructure();

        // process dynamic meta
        this.processBoDynamicMetas();
    }

    public Integer getCommondCode4Create() {
        return 201;
    }

    @Override
    public void create() {
        BOPermissionChecker.getChecker().checkBOCreate(boMeta.getNamespace(), boMeta.getName(), boFacade);

        Integer commondCode4Create = getCommondCode4Create();
        this.doCommand(commondCode4Create);

        // process dynamic meta
        this.processBoDynamicMetas();

        this.postCreate();
    }

    private void postCreate() {
        for (BusinessObjectExitHandler interceptor : exitHandlers) {
            interceptor.postCreate(boFacade, this);
        }
    }

    public Integer getCommondCode4Update() {
        return 202;
    }

    @Override
    public void update() {
        BOPermissionChecker.getChecker().checkBOUpdate(boMeta.getNamespace(), boMeta.getName(), boFacade);

        Integer commondCode4Update = getCommondCode4Update();
        this.doCommand(commondCode4Update);

        // process dynamic meta
        this.processBoDynamicMetas();

        this.postUpdate();
    }

    private void postUpdate() {
        for (BusinessObjectExitHandler interceptor : exitHandlers) {
            interceptor.postUpdate(boFacade, this);
        }
    }

    public Integer getCommondCode4Remove() {
        return 203;
    }

    @Override
    public void delete() {
        BOPermissionChecker.getChecker().checkBODelete(boMeta.getNamespace(), boMeta.getName(), boFacade);

        Integer commondCode4Remove = getCommondCode4Remove();
        this.doCommand(commondCode4Remove);

        postDelete();
        LOGGER.info("Delete----------------------------" + this.getIdentifier());
    }

    private void postDelete() {
        for (BusinessObjectExitHandler interceptor : exitHandlers) {
            interceptor.postDelete(boFacade, this);
        }
    }

    public void remove() {
        delete();
    }

    @Override
    public void onBOChange(OnBOChangeParam param) {
        BOPermissionChecker.getChecker().checkReadOnly(boMeta.getNamespace(), boMeta.getName(), boFacade);

        new DefaultOnBOChangeExecutor().onBOChange(this, boMeta, param);
    }

    @Override
    public BusinessObjectIdentifier getIdentifier() {
        if (this.boIdentifier == null || !this.boIdentifier.isValid()) {
            List<? extends PropertyRef> keys = boMeta.getRootNode().getPrimaryKey();
            Map<String, Object> keyMap = new HashMap<String, Object>(keys.size());
            for (PropertyRef key : keys) {
                Object value = this.getPropertyValue(key.getName());
                keyMap.put(key.getName(), value);
            }

            BOImplementation boAnno = this.getBO().getPureJavaType().getAnnotation(BOImplementation.class);
            String boNamespace = boAnno.namespace();
            String boName = boAnno.name();
            this.boIdentifier = new BusinessObjectIdentifier(boNamespace, boName, keyMap);
        }
        return this.boIdentifier;
    }

    DefaultBOActionExecutor actionExecutor;

    @Override
    public Object executeAction(String actionName, Object... parameters) {
        if (actionExecutor == null) {
            actionExecutor = new DefaultBOActionExecutor(this.boMeta);
        }
    System.out.println( "====>run ruby script before action "+actionName);
    try{
        loadRuby();
    //  String ruby_root = "/var/sa/ruby/";
    //  jruby.eval(new BufferedReader(new FileReader(ruby_root+"main.rb")));
        String r_s = "loadObject(\"" + this.getClass().getName()+"\" ,nil, '/var/sa/ruby/root/').before_action_"+actionName;
        System.out.println("run ruby script:"+r_s);
        jruby.eval("puts '=>>>hello call ruby ok'");
        jruby.eval(r_s);
        System.out.println("run ruby extension OK.");
    }catch(Throwable e){
        System.out.println("WARN: run ruby script failed:");
        e.printStackTrace();
    }
        return actionExecutor.executeAction(this, actionName, parameters);
    }

    protected void invokeBOChange(String actionName, BusinessObjectNode node) {
        BOPermissionChecker.getChecker().checkReadOnly(boMeta.getNamespace(), boMeta.getName(), boFacade);

        if (null != node)
            this.setLineNumberArgument(node);

        OnBOChange boChange = this.boMeta.getRootNode().getBOChangeByName(actionName);
        if (boChange == null)
            throw new SystemException(BOFrwJawErrorCode.INVALID_ACTION_NAME,actionName);

        this.doCommand(boChange.getImplCommandCode().longValue());
        // refresh node structure
        refreshNodeStructure();

        // process dynamic meta
        this.processBoDynamicMetas();
    }

    @Override
    public BusinessObject getBO() {
        return this;
    }

    @Override
    public BusinessObject getAssociatedBusinessObject(String associationName) {
        // TODO: check associationName and call the corresponding method
        // accordingly!!
        List<? extends Association> associationList = boMeta.getRootNode().getAssociations();
        for (Association association : associationList) {
            if (association.getName().equals(associationName)) {
                    Method method = null;
					try {
						method = this.getClass().getMethod("get" + StringUtils.capitalize(associationName));
						return ((BusinessObject) method.invoke(this));
					} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
						LOGGER.error("ObserverBusinessObject.getAssociatedBusinessObject exception : {}",e.getLocalizedMessage());
						throw new SystemException(BOFrwJawErrorCode.METHOD_INVOKE_ERROR,e);
					}
            }
        }
        throw new BusinessException(BOFrwJawErrorCode.BO_ASSOCIATION_NOT_DEFINE, this.getBoMeta().getName(),associationName);
    }

    @Override
    public void associateBusinessObject(String associationName, BusinessObject boToAssociate) {
        // TODO: check associationName and call the corresponding method
        // accordingly!!
        List<? extends Association> associationList = boMeta.getRootNode().getAssociations();
        for (Association association : associationList) {
            if (association.getName().equals(associationName)) {
                try {
                    Method getter = this.getClass().getMethod("get" + StringUtils.capitalize(associationName));
                    Method setter = this.getClass().getMethod("set" + StringUtils.capitalize(associationName),
                            getter.getReturnType());
                    setter.invoke(this, boToAssociate);
                    return;
                } catch (InvocationTargetException e) {
                    Throwable t = e.getTargetException();
                    if (t instanceof BaseException) {
                        throw (BaseException) t;
                    } else {
                        throw new SystemException(BOFrwJawErrorCode.UNKNOWN_SYSTEM_ERROR, e,e.getMessage());
                    }
                } catch (Exception e) {
                    throw new SystemException(BOFrwJawErrorCode.UNKNOWN_SYSTEM_ERROR, e,e.getMessage());
                }
            }
        }
        throw new BusinessException(BOFrwJawErrorCode.BO_ASSOCIATION_NOT_DEFINE, this.getBoMeta().getName(),associationName);
    }

    protected <T extends ObserverBusinessObjectParam> T invokeAction(String actionName, Class<T> clazz,
            Object... actionParameters) {
        BOPermissionChecker.getChecker().checkBOAction(boMeta.getNamespace(), boMeta.getName(), actionName, boFacade);

        Action actionMD = this.boMeta.getRootNode().getAction(actionName);
        if (actionMD == null)
            throw new SystemException(BOFrwJawErrorCode.INVALID_ACTION_NAME,actionName);

        List<? extends InputParameter> inputParamMDList = actionMD.getInputParameters();
        if (actionParameters.length != inputParamMDList.size())
            throw new SystemException(BOFrwJawErrorCode.WRONG_ACTION_PARAMS_NUMBER,actionName);

        // set action parameter
        int i = 0;
        for (InputParameter inputParamMD : inputParamMDList) {
            ObserverBusinessObjectParamProxy paramProxy = getParameterProxy(inputParamMD.getType());
            if (actionParameters[i] instanceof ObserverBusinessObjectParam) {
                ((ObserverBusinessObjectParam) actionParameters[i]).transformTo(paramProxy);
            } else if (actionParameters[i] instanceof List) {
                ObserverBusinessobjectParamListTransformer.transformTo((List) actionParameters[i], paramProxy);
            }
            i++;
        }

        BigInteger cmdCode = actionMD.getImplCommandCode();
        this.doCommand(cmdCode.longValue());

        // process dynamic meta
        this.processBoDynamicMetas();

        if (clazz == null)
            return null;

        OutputParameter outputParamMD = actionMD.getOutputParameter();
        try {
            T paramObj = clazz.newInstance();
            ObserverBusinessObjectParamProxy outParamProxy = getParameterProxy(outputParamMD.getType());
            paramObj.transformFrom(outParamProxy);
            return paramObj;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new SystemException(BOFrwJawErrorCode.UNKNOWN_SYSTEM_ERROR, e,e.getMessage());
        }
    }

    protected <T extends ObserverBusinessObjectParam> List<T> invokeAction2(String actionName, Class<T> itemClazz,
            Object... actionParameters) {
        BOPermissionChecker.getChecker().checkBOAction(boMeta.getNamespace(), boMeta.getName(), actionName, boFacade);

        Action actionMD = this.boMeta.getRootNode().getAction(actionName);
        if (actionMD == null)
            throw new SystemException(BOFrwJawErrorCode.INVALID_ACTION_NAME,actionName);

        List<? extends InputParameter> inputParamMDList = actionMD.getInputParameters();
        if (actionParameters.length != inputParamMDList.size())
            throw new SystemException(BOFrwJawErrorCode.WRONG_ACTION_PARAMS_NUMBER,actionName);

        // set action parameter
        int i = 0;
        for (InputParameter inputParamMD : inputParamMDList) {
            ObserverBusinessObjectParamProxy paramProxy = getParameterProxy(inputParamMD.getType());
            if (actionParameters[i] instanceof ObserverBusinessObjectParam) {
                ((ObserverBusinessObjectParam) actionParameters[i]).transformTo(paramProxy);
            } else if (actionParameters[i] instanceof List) {
                ObserverBusinessobjectParamListTransformer.transformTo((List) actionParameters[i], paramProxy);
            }
            i++;
        }

        BigInteger cmdCode = actionMD.getImplCommandCode();
        this.doCommand(cmdCode.longValue());

        // process dynamic meta
        this.processBoDynamicMetas();

        if (itemClazz == null)
            return null;

        List<T> list = new ArrayList<T>();
        OutputParameter outputParamMD = actionMD.getOutputParameter();
        ObserverBusinessObjectParamProxy outParamProxy = getParameterProxy(outputParamMD.getType());
        try {
            int count = outParamProxy.getRowCount();
            for (int j = 0; j < count; j++) {
                outParamProxy.setCurrentRow(j);
                T paramObj = itemClazz.newInstance();
                paramObj.transformFrom(outParamProxy);
                list.add(paramObj);
            }
        } catch (Exception e) {
            throw new BusinessException(e);
        }
        return list;
    }

    protected Recordset invokeRecordset(String actionName, Object... actionParameters) {
        Recordset recordset = null;
        Action actionMD = this.boMeta.getRootNode().getAction(actionName);

        List<? extends InputParameter> inputParamMDList = actionMD.getInputParameters();
        if (actionParameters.length != inputParamMDList.size())
            throw new SystemException(BOFrwJawErrorCode.WRONG_ACTION_PARAMS_NUMBER,actionName);

        // set action parameter
        int i = 0;
        for (InputParameter inputParamMD : inputParamMDList) {
            ObserverBusinessObjectParamProxy paramProxy = getParameterProxy(inputParamMD.getType());
            if (actionParameters[i] instanceof ObserverBusinessObjectParam) {
                ((ObserverBusinessObjectParam) actionParameters[i]).transformTo(paramProxy);
            } else if (actionParameters[i] instanceof List) {
                ObserverBusinessobjectParamListTransformer.transformTo((List) actionParameters[i], paramProxy);
            }
            i++;
        }

        // let's do command
        BigInteger cmdCode = actionMD.getImplCommandCode();
        this.doCommand(cmdCode.longValue());

        // let's create the service data which contains metadata
        ProxyServiceData psd = this.proxyService.createProxyServiceDataWrapper(RECORDSET);

        // how many fields the query returned
        int fieldCount = psd.getFieldCount();
        recordset = new Recordset();
        // how many rows the query returned
        int rowCount = psd.getLogicRowCount();
        String alias = null;
        String value = null;
        char fieldType = 0;
        for (int index = 0; index < rowCount; index++) {
            psd.setCurrentRow(index);
            Map<String, String> rowData = new HashMap<String, String>();
            for (int f = 0; f < fieldCount; f++) {
                alias = psd.getFieldAliasByIndex(f);
                fieldType = (char) psd.getFieldType(alias);
                switch (fieldType) {
                case 'A':
                    value = psd.getFieldValueString(alias);
                    break;
                case 'L':
                    value = psd.getFieldValueString(alias);
                    break;
                case 'N':
                    value = psd.getFieldValueString(alias);
                    break;
                case 'D':
                    value = psd.getFieldValueString(alias);
                    break;
                case 'B':
                    value = String.valueOf(psd.getFieldValueDouble(alias));
                    break;
                case 'M':
                    value = psd.getFieldValueMemo(alias);
                    break;
                default:
                    value = psd.getFieldValueString(alias);
                }
                rowData.put(alias, value);
            }
            recordset.addRowData(rowData);
        }

        return recordset;
    }

    @Override
    public void release() {
        if (!isReleased) {
            this.proxyService.release();
            isReleased = true;
        }

    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

    public final static String BUILT_IN_COMPLEX_TYPE_BO_DYNAMIC_META = "BODynamicMeta";

    protected void processBoDynamicMetas() {
        // After do command, we will get dynamic metadata
        this.boDynamicMetas = getParameterProxy(BUILT_IN_COMPLEX_TYPE_BO_DYNAMIC_META);
        for (int i = 0; i < boDynamicMetas.getRowCount(); i++) {
            this.boDynamicMetas.setCurrentRow(i);
            // property is actually column name
            String property = (String) this.boDynamicMetas.getPropertyValue("property");
            String editable = (String) this.boDynamicMetas.getPropertyValue("editable");
            // bONode is actually table name
            String tableName = (String) this.boDynamicMetas.getPropertyValue("bONode");
            Integer rowNum = (Integer) boDynamicMetas.getPropertyValue("rowNumber");

            LOGGER.debug("Got dynamic metadata for property: " + property);

            if (tableName != null && "Actions".equals(tableName)) {
                // process action dynamic meta
                String actionName = this.boMeta.getRootNode().getActionByImplCode(Integer.parseInt(property)).getName();
                ActionDynamicMeta actionMeta = this.dynamicMeta.getAction(actionName);
                actionMeta.setEnabled("Y".equalsIgnoreCase(editable));
            } else if (tableName != null && tableName.equals(this.boMeta.getRootNode().getImplTable())) {
                // process root node dynamic meta
                PropertyDynamicMeta propertyDynamicMeta = this.dynamicMeta.getProperty(this.boMeta.getRootNode()
                        .getPropertyByColumnName(property).getName());
                propertyDynamicMeta.setReadOnly(!"Y".equalsIgnoreCase(editable));
            } else {
                // process sub node dynamic meta
                List<? extends NodeType> nodeTypeList = boMeta.getNodeTypeByTableName(tableName);
                if (nodeTypeList.size() > 0) {
                    for (NodeType nodeType : nodeTypeList) {
                        // TODO at moment not support node hierarchy more than
                        // 2.
                        Node node = boMeta.getRootNode().getNodeByNodeType(nodeType.getName());
                        if (rowNum == -1) {
                            // -1 means apply dynamic meta for all rows
                            SubNodeDynamicMeta subNodeMeta = this.dynamicMeta.getSubNodeMeta(node.getName());
                            PropertyDynamicMeta propertyDynamicMeta = null;
                            if (PropertyDynamicMeta.WILD_CHAR.equals(property)) {
                                propertyDynamicMeta = subNodeMeta.getProperty(PropertyDynamicMeta.WILD_CHAR);
                                propertyDynamicMeta.setEnabled(true);
                            } else {
                                Property propertyMeta = nodeType.getPropertyByColumnName(property);
                                if (propertyMeta != null) {
                                    propertyDynamicMeta = subNodeMeta.getProperty(nodeType.getPropertyByColumnName(
                                            property).getName());
                                }
                            }

                            if (propertyDynamicMeta != null) {
                                propertyDynamicMeta.setReadOnly(!"Y".equalsIgnoreCase(editable));
                                // TODO temporary logic, set sub node readOnly =
                                // true, if row num == -1, C++ may need make the
                                // interface more clear in future
                                if (PropertyDynamicMeta.WILD_CHAR.equals(property) && !"Y".equalsIgnoreCase(editable)) {
                                    subNodeMeta.setReadOnly(true);
                                }
                            }
                        } else {
                            BusinessObjectNodeList<ObserverBusinessObjectNode> boNodeList = this.getNodeList(node
                                    .getName());
                            for (ObserverBusinessObjectNode boNode : boNodeList) {
                                if (rowNum.equals(boNode.getCurretRow())) {
                                    PropertyDynamicMeta propertyDynamicMeta = null;
                                    if (PropertyDynamicMeta.WILD_CHAR.equals(property)) {
                                        propertyDynamicMeta = boNode.getDynamicMeta().getProperty(
                                                PropertyDynamicMeta.WILD_CHAR);
                                        propertyDynamicMeta.setEnabled(true);
                                    } else {
                                        Property propertyMeta = nodeType.getPropertyByColumnName(property);
                                        if (propertyMeta != null) {
                                            propertyDynamicMeta = boNode.getDynamicMeta().getProperty(
                                                    nodeType.getPropertyByColumnName(property).getName());
                                        }
                                    }
                                    if (propertyDynamicMeta != null) {
                                        propertyDynamicMeta.setReadOnly(!"Y".equalsIgnoreCase(editable));
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public ObserverBusinessObjectParamProxy getBoDynamicMetas() {
        return this.boDynamicMetas;
    }

    private void setLineNumberArgument(BusinessObjectNode node) {
        ComplexType complexType = TypeUtils.getComplexType("Argument");
        if (complexType != null) {
            ObserverBusinessObjectParamProxy paramProxy = getParameterProxy(complexType.getName());
            paramProxy.clear();
            paramProxy.addRow();
            paramProxy.setPropertyValue("argName", "LineNum");
            ObserverBusinessObjectNode targetNode;
            if (node instanceof ObserverBusinessObject)
                targetNode = ((ObserverBusinessObject) node).rootNode;
            else
                targetNode = (ObserverBusinessObjectNode) node;
            paramProxy.setPropertyValue("argValue", String.valueOf(targetNode.getServiceData().getPhysicalRow()));
        }
    }

    ProxyServiceData createProxyServiceData(String serviceDataTable) {
        return this.proxyService.createProxyServiceDataWrapper(serviceDataTable);
    }

    @Override
    public void addExitHandler(BusinessObjectExitHandler exitHandler) {
        this.exitHandlers.add(exitHandler);
    }

    @Override
    public void onShare(Integer targetId, PermissionLevel level, boolean isEmployeeId) {
    }

    protected void validateBoProperties() {
        ValidationRepository validRepo = boFacade.getValidationRepository();
        if (validRepo == null) {
            return;
        }

        validRepo.validateBONodeProperties(boFacade, this, boMeta.getRootNode());
    }

    @SuppressWarnings({ "rawtypes", "unchecked"})
	protected void validateUniqueProperties() {
    	NodeType nodeType = this.boMeta.getRootNode();

    	if (nodeType != null) {
    		String pkName = null;
        	Object pkValue = null;

        	List<? extends PropertyRef> pkList = nodeType.getPrimaryKey();
        	if (pkList.isEmpty()) {
        		LOGGER.error("no primary key found for nodeType: {} ", nodeType.getName());
        	} else {
        		pkName = pkList.get(0).getName();
        		pkValue = this.getPropertyValue(pkName);
        	}

    		for (Property prop : nodeType.getProperties()) {
    			String propName = prop.getName();
        		if (prop.isUnique() && prop.isEnabled() && !propName.equals(pkName)) {
                    Object propValue = getPropertyValue(propName);
                    if (propValue == null) {
                        continue;
                    }

                    Criteria cr = new Criteria();
                    Predicate eq = new Equal(new Path(String.class, propName), new Constant(propValue));
                    Predicate cond = eq;
                    if (pkValue != null) {
					    // eliminate itself from query
                    	cond = new And(eq, new NotEqual(new Path(String.class, pkName), new Constant(pkValue)));
                    }

                    cr.where(cond);
                    long cnt = boFacade.getBORepository(this.getClass()).count(cr);
                    if (cnt > 0) {
                        throw new LocatableBusinessException(propName, BOFrwJawErrorCode.UDF_UNIQUE, prop.getLabel());
                    }
                }
            }
        }

    }

    @Override
    public BusinessObjectIdentifier getBusinessKey() {
        BusinessObjectIdentifier bkIdentifier = null;
        List<? extends PropertyRef> keys = boMeta.getRootNode().getBusinessKey();
        if (null == keys || keys.size() <= 0) {
            keys = boMeta.getRootNode().getPrimaryKey();
        }
        if (null != keys && keys.size() > 0) {
            Map<String, Object> keyMap = new HashMap<String, Object>(keys.size());
            for (PropertyRef key : keys) {
                Object value = this.getPropertyValue(key.getName());
                keyMap.put(key.getName(), value);
            }

            BOImplementation boAnno = this.getBO().getPureJavaType().getAnnotation(BOImplementation.class);
            String boNamespace = boAnno.namespace();
            String boName = boAnno.name();
            bkIdentifier = new BusinessObjectIdentifier(boNamespace, boName, keyMap);
        }
        return bkIdentifier;
    }

    @Override
    public Class<?> getPureJavaType() {
        return ClassHelper.getOriginalType(this.getClass());
    }

    public String getInternalFieldsValue(){
    	return rootNode.getInternalFieldsValue();
    }

    public void setInternalFieldsValue(String values){
        if(StringUtils.isNotEmpty(values)){
        	this.byPassAutoComplete = true;
            rootNode.setInternalFieldsValue(values);
        }
    }

    public void copyUserFieldsFrom(BusinessObject bo, NodeType rootNode) {
    	this.rootNode.copyUserFieldsFrom(bo, rootNode);
    }
}
