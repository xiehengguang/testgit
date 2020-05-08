package com.sapdev.service.impl;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.management.RuntimeErrorException;

import org.apache.commons.lang.StringUtils;

import com.sap.me.browse.BrowseSfcRequest;
import com.sap.me.browse.BrowseSfcResponse;
import com.sap.me.browse.BrowseSfcServiceInterface;
import com.sap.me.common.AttributeValue;
import com.sap.me.common.CustomValue;
import com.sap.me.common.ObjectAliasEnum;
import com.sap.me.common.ObjectReference;
import com.sap.me.datacollection.CreateParametricMeasure;
import com.sap.me.datacollection.CreateParametricRequest;
import com.sap.me.datacollection.DCGroupToCollectResponse;
import com.sap.me.datacollection.DCGroupsToCollectResponse;
import com.sap.me.datacollection.DCParameterResponse;
import com.sap.me.datacollection.DataCollectionServiceInterface;
import com.sap.me.datacollection.DcGroupsForResourceRequest;
import com.sap.me.datacollection.DcGroupsForWorkCenterRequest;
import com.sap.me.demand.ReleaseShopOrderRequest;
import com.sap.me.demand.ReleaseShopOrderResponse;
import com.sap.me.demand.ReleasedSfc;
import com.sap.me.demand.SFCBOHandle;
import com.sap.me.demand.SfcIdentifier;
import com.sap.me.demand.ShopOrderBOHandle;
import com.sap.me.demand.ShopOrderBasicConfiguration;
import com.sap.me.demand.ShopOrderFullConfiguration;
import com.sap.me.demand.ShopOrderInputException;
import com.sap.me.demand.ShopOrderNotFoundException;
import com.sap.me.demand.ShopOrderServiceInterface;
import com.sap.me.extension.Services;
import com.sap.me.frame.BOHandle;
import com.sap.me.frame.BasicBOBeanException;
import com.sap.me.frame.Data;
import com.sap.me.frame.Utils;
import com.sap.me.frame.domain.BusinessException;
import com.sap.me.nonconformance.CreateNCRequest;
import com.sap.me.nonconformance.CreateNCResponse;
import com.sap.me.nonconformance.DispositionRequest;
import com.sap.me.nonconformance.DispositionSelection;
import com.sap.me.nonconformance.NCCodeBOHandle;
import com.sap.me.nonconformance.NCProductionServiceInterface;
import com.sap.me.nonconformance.ProductionContext;
import com.sap.me.nonconformance.StepIdentifier;
import com.sap.me.numbering.GenerateNextNumberRequest;
import com.sap.me.numbering.GenerateNextNumberResponse;
import com.sap.me.numbering.NextNumberTypeEnum;
import com.sap.me.numbering.NumberingServiceInterface;
import com.sap.me.plant.ResourceBOHandle;
import com.sap.me.plant.WorkCenterBOHandle;
import com.sap.me.productdefinition.AttachedToContext;
import com.sap.me.productdefinition.AttachmentConfigurationServiceInterface;
import com.sap.me.productdefinition.AttachmentType;
import com.sap.me.productdefinition.BOMComponentConfiguration;
import com.sap.me.productdefinition.BOMConfigurationServiceInterface;
import com.sap.me.productdefinition.BOMFullConfiguration;
import com.sap.me.productdefinition.FindAttachmentByAttachedToContextRequest;
import com.sap.me.productdefinition.FindAttachmentByAttachedToContextResponse;
import com.sap.me.productdefinition.FindOperationCurrentRevisionRequest;
import com.sap.me.productdefinition.FoundReferencesResponse;
import com.sap.me.productdefinition.ItemBOHandle;
import com.sap.me.productdefinition.ItemBasicConfiguration;
import com.sap.me.productdefinition.ItemConfigurationServiceInterface;
import com.sap.me.productdefinition.ItemFullConfiguration;
import com.sap.me.productdefinition.ItemSearchRequest;
import com.sap.me.productdefinition.ItemSearchResult;
import com.sap.me.productdefinition.OperationBOHandle;
import com.sap.me.productdefinition.OperationConfigurationServiceInterface;
import com.sap.me.productdefinition.OperationFullConfiguration;
import com.sap.me.productdefinition.ReadBOMRequest;
import com.sap.me.productdefinition.WorkInstructionConfigurationServiceInterface;
import com.sap.me.productdefinition.WorkInstructionFullConfiguration;
import com.sap.me.productdefinition.WorkInstructionNotFoundException;
import com.sap.me.production.AssembleComponentsRequest;
import com.sap.me.production.AssembleComponentsResponse;
import com.sap.me.production.AssembleNonBomComponentsRequest;
import com.sap.me.production.AssemblyComponent;
import com.sap.me.production.AssemblyDataField;
import com.sap.me.production.AssemblyServiceInterface;
import com.sap.me.production.CollectSfcDataRequest;
import com.sap.me.production.CompleteSfcQuickRequest;
import com.sap.me.production.CompleteSfcRequest;
import com.sap.me.production.CreateSfcRequest;
import com.sap.me.production.CreateSfcResponse;
import com.sap.me.production.CreateSfcServiceInterface;
import com.sap.me.production.FindSfcByNameRequest;
import com.sap.me.production.FindSfcDataBySfcRequest;
import com.sap.me.production.FindSfcDataBySfcResponse;
import com.sap.me.production.SfcBasicData;
import com.sap.me.production.SfcCompleteServiceInterface;
import com.sap.me.production.SfcDataField;
import com.sap.me.production.SfcDataServiceInterface;
import com.sap.me.production.SfcStartServiceInterface;
import com.sap.me.production.SfcStateServiceInterface;
import com.sap.me.production.SfcStep;
import com.sap.me.production.SfcStepStatusEnum;
import com.sap.me.production.SignoffRequest;
import com.sap.me.production.SignoffServiceInterface;
import com.sap.me.production.SignoffSfcData;
import com.sap.me.production.StartSfcRequest;
import com.sap.me.production.StartSfcResponse;
import com.sap.me.production.ValidateSfcAtOperationRequest;
import com.sap.me.user.UserBOHandle;
import com.sap.tc.logging.Category;
import com.sap.tc.logging.Location;
import com.sap.tc.logging.Severity;
import com.sap.tc.logging.SimpleLogger;
import com.sapdev.exception.ExtBusinessException;
import com.sapdev.service.DataCoreServiceInterface;
import com.sapdev.service.SfcOperationServiceInterface;
import com.sapdev.service.dto.DCData;
import com.sapdev.service.dto.LogicQueryResponse;
import com.sapdev.service.dto.SfcOperationRequest;
import com.sapdev.service.dto.SfcOperationResponse;
import com.sapdev.service.dto.SqlQueryRequest;
import com.sapdev.utils.HelperUtil;

public class SfcOperationService implements SfcOperationServiceInterface {
    private static final String SFC_PACKAGE = "com.sap.me.production";
    private static final String SFC_START_SERVICE = "SfcStartService";
    private static final String SFC_COMPLETE_SERVICE = "SfcCompleteService";
    private static final String STATESERVICE = "SfcStateService";
    
    private static final Category category = Category.getCategory(Category.APPLICATIONS, "/ME/Extension/Execution");
  	private static final Location loc = Location.getLocation("com.sapdev.service.impl.SfcOperationService");
  	private static final String MESSAGE_ID = "SfcOperationService";

    //根据工序，资源或工单返回相关状态的sfc列表
    @Override
    public List<Map> getSfcList(SfcOperationRequest request) throws BusinessException {
        List<Map> sfcList = new ArrayList<Map>();
        String site = request.getSite();
        StringBuffer sb = new StringBuffer();
        sb.append("select s.handle, s.sfc,i.item, sst.qty_in_work,s.priority,sst.date_queued, so.shop_order");
        sb.append(" from sfc_step sst,sfc_router sr,sfc_routing srt,sfc s,sfc_in_work siw, item i, shop_order so, status st ");
        sb.append(" where sst.sfc_router_bo=sr.handle and srt.handle=sr.sfc_routing_bo and s.handle=srt.sfc_bo");
        sb.append(" and s.handle=srt.sfc_bo and siw.sfc_step_bo=sst.handle AND replace(s.item_bo, '#', i.revision)=i.handle");
        sb.append(" AND i.current_revision='true' AND s.shop_order_bo=so.handle AND s.status_bo=st.handle");
        sb.append(" AND s.site='").append(site).append("'");
        if (request.getOperation()!=null && !request.getOperation().equals("")) {
            String oper = "OperationBO:"+site+","+request.getOperation()+"%";
            sb.append(" AND sst.operation_bo LIKE '").append(oper).append("'");
        }
        if (request.getResource()!=null && !request.getResource().equals("")) {
            String reso = "ResourceBO:"+site+","+request.getResource()+"";
            sb.append(" and siw.resource_bo='").append(reso).append("'");
        }
        if (request.getStatus()!=null && !request.getStatus().equals("")) {
            sb.append(" AND st.status='").append(request.getStatus()).append("'");
        } else {
            sb.append(" AND st.status='403'");  //缺省查询活动的sfc
        }
        if (request.getSfc()!=null && !request.getSfc().equals("")) {
            sb.append(" s.sfc '").append(request.getSfc()).append("%'");
        }
        if (request.getShoporder()!=null && !request.getShoporder().equals("")) {
            sb.append(" so.shop_order like '").append(request.getShoporder()).append("%'");
        }
        LogicService lservice = new LogicService();
        SqlQueryRequest sqlrequest = new SqlQueryRequest();
        sqlrequest.setSql(sb.toString());
        LogicQueryResponse response = lservice.query(sqlrequest);
        sfcList = response.getRecords();
        
        return sfcList;
    }

    /**
     * @return 
     * 启动SFC
     * @param [types] resource:资源; operation:操作
     * @return String 
     * @throws  
     */ 
    public void Start(String site, String userid, String operation, String resource, String sfc) throws BusinessException{
        SfcStartServiceInterface sfcStartService = Services.getService(SFC_PACKAGE, SFC_START_SERVICE, site);
        StartSfcRequest sfcreq = new StartSfcRequest();     
        BOHandle operHandle = new OperationBOHandle(site, operation, "#");//操作
        BOHandle resourceHandle = new ResourceBOHandle(site, resource);//资源
        String resourceRef = resourceHandle.getValue();
        String operationRef = operHandle.getValue();
        BOHandle sfcHandle = new SFCBOHandle(site, sfc);//SFC
        BOHandle userRef = new UserBOHandle(site,userid);
        sfcreq.setSfcRef(sfcHandle.getValue());
        sfcreq.setOperationRef(operationRef);
        sfcreq.setResourceRef(resourceRef);
        sfcreq.setNoSfcValidation(true);
        sfcreq.setUserRef(userRef.getValue());
        List<StartSfcRequest> sfcList = new ArrayList<StartSfcRequest>();
        sfcList.add(sfcreq);
        long startTime = System.currentTimeMillis();
        sfcStartService.start(sfcList);     
        long totalTime = System.currentTimeMillis() - startTime;
		
		if(totalTime>1000){
			SimpleLogger.log(Severity.WARNING, category, loc, MESSAGE_ID, 
					String.format("Start Sfc, total speed time %s, SfcStartServiceInterface, sfc [%s], operation [%s], resrce[%s]", 
							new Object[] {String.valueOf(totalTime), 
									sfc, operation, resource }));
		}else{
			SimpleLogger.log(Severity.INFO, category, loc, MESSAGE_ID, 
					String.format("Start Sfc, total speed time %s, SfcStartServiceInterface, sfc [%s], operation [%s], resrce[%s]", 
							new Object[] {String.valueOf(totalTime),
									sfc, operation, resource }));
		}
    }

    /**
     * 完成SFC
     * @param [types] resource:资源; operation:操作
     * @return String 
     * @throws  
     */ 
    public void Complete(String site, String userid, String operation, String resource, String sfc) throws BusinessException{
        SfcCompleteServiceInterface sfcCompleteService = Services.getService(SFC_PACKAGE, SFC_COMPLETE_SERVICE, site);
        CompleteSfcRequest sfcreq = new CompleteSfcRequest();       
        BOHandle operHandle = new OperationBOHandle(site, operation, "#");  //操作
        BOHandle resourceHandle = new ResourceBOHandle(site, resource);  //资源
        String resourceRef = resourceHandle.getValue();
        String operationRef = operHandle.getValue();
        BOHandle sfcHandle = new SFCBOHandle(site, sfc);  //SFC
        sfcreq.setSfcRef(sfcHandle.getValue());
        sfcreq.setOperationRef(operationRef);
        sfcreq.setResourceRef(resourceRef);
        long startTime = System.currentTimeMillis();
        sfcCompleteService.completeSfc(sfcreq);     
        long totalTime = System.currentTimeMillis() - startTime;
		
		if(totalTime>1000){
			SimpleLogger.log(Severity.WARNING, category, loc, MESSAGE_ID, 
					String.format("Complete Sfc, total speed time %s, SfcCompleteServiceInterface, sfc [%s], operation [%s], resrce[%s]", 
							new Object[] {String.valueOf(totalTime), 
									sfc, operation, resource }));
		}else{
			SimpleLogger.log(Severity.INFO, category, loc, MESSAGE_ID, 
					String.format("Complete Sfc, total speed time %s, SfcCompleteServiceInterface, sfc [%s], operation [%s], resrce[%s]", 
							new Object[] {String.valueOf(totalTime),
									sfc, operation, resource }));
		}
    }
    
    /**
     * 通过SFC
     * @param [types] resource:资源; operation:操作
     * @return String 
     * @throws  
     */     
    public String Pass(String site, String userid, String operation, String resource, String sfc){
        String returnInf = this.CheckSfcStatus(site, operation, resource, sfc, "402");
        String operationRef = "OperationBO:"+site+","+operation+",#";
        BOHandle resourceHandle = new ResourceBOHandle(site, resource);//资源
        String resourceRef = resourceHandle.getValue();
        BOHandle sfcHandle = new SFCBOHandle(site, sfc);//SFC
        List<SignoffSfcData> sfcData = new ArrayList<SignoffSfcData>();
        if (returnInf.indexOf("E:")>-1) {
            return returnInf;
        } else {
            if (returnInf.equalsIgnoreCase("I:["+sfc+"]状态检查符合")) {
                SfcStartServiceInterface sfcStartService = Services.getService(SFC_PACKAGE, SFC_START_SERVICE, site);
                StartSfcRequest sfcreq = new StartSfcRequest();     
                //BOHandle operHandle = new OperationBOHandle(site, operation, revision);//操作
                BOHandle userRef = new UserBOHandle(site,userid);
                sfcreq.setSfcRef(sfcHandle.getValue());
                sfcreq.setOperationRef(operationRef);
                sfcreq.setResourceRef(resourceRef);
                sfcreq.setNoSfcValidation(true);
                sfcreq.setUserRef(userRef.getValue());
                List<StartSfcRequest> sfcList = new ArrayList<StartSfcRequest>();
                sfcList.add(sfcreq);                
                
                try {
                    Collection<StartSfcResponse> sfcColl = sfcStartService.start(sfcList);
                    for (StartSfcResponse sfcResp:sfcColl) {
                        SignoffSfcData singoff = new SignoffSfcData();
                        singoff.setSfcRef(sfcResp.getSfcRef());
                        singoff.setOperationRef(operationRef);
                        singoff.setResourceRef(resourceRef);
                        sfcData.add(singoff);
                    }
                } catch (Exception e) {
                    String errinf = site+","+userid+","+operation+","+resource+","+sfc+":"+e.toString();
                    SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, Location.getLocation(SfcOperationService.class), "start", errinf);
                    return "E:"+errinf;
                }           
            } 
            returnInf = this.CheckSfcStatus(site, operation, resource, sfc, "403");

            if (returnInf.contains("E:")) {
                return returnInf;
            }
            
            SfcCompleteServiceInterface sfcCompleteService = Services.getService(SFC_PACKAGE, SFC_COMPLETE_SERVICE, site);
            CompleteSfcRequest sfcreqcom = new CompleteSfcRequest();        

            sfcreqcom.setSfcRef(sfcHandle.getValue());
            sfcreqcom.setOperationRef(operationRef);
            sfcreqcom.setResourceRef(resourceRef);
            
            try {
                sfcCompleteService.completeSfc(sfcreqcom);
                returnInf = "I:["+sfc+"]加工完成，进入下一工序";
            } catch (Exception e) {
                String errinf = site+","+userid+","+operation+","+resource+","+sfc+":"+e.toString();
                SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, Location.getLocation(SfcOperationService.class), "Complete", errinf);
                
                SignoffServiceInterface signoffs = Services.getService(SFC_PACKAGE, "SignoffService", site);
                SignoffRequest signoffRequest = new SignoffRequest();
                signoffRequest.setSfcData(sfcData);
                try {
                    signoffs.signoffSfc(signoffRequest);
                } catch (Exception ex) {
                    errinf = site+","+userid+","+operation+","+resource+","+sfc+":"+ex.toString();
                    SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, Location.getLocation(SfcOperationService.class), "Signoff", errinf);
                }
                returnInf = "E:"+e.getMessage();
            } 
        }

        return returnInf;
        
    }
    //让已经开始的SFC执行暂停操作
    public String SignOff(String site, String userid, String operation, String resource, String sfc) throws BusinessException {
        String ret = "S";
        if (!this.isInWork(site, operation, sfc)) {
            return "此SFC所在状态不能进行暂停操作！";
        }
        SignoffServiceInterface signoffs = Services.getService(SFC_PACKAGE, "SignoffService", site);
        SignoffRequest signoffRequest = new SignoffRequest();
        List<SignoffSfcData> sfcData = new ArrayList<SignoffSfcData>();
        SignoffSfcData singoff = new SignoffSfcData();
        BOHandle operHandle = new OperationBOHandle(site, operation, "#");  //操作
        BOHandle resourceHandle = new ResourceBOHandle(site, resource);  //资源
        String resourceRef = resourceHandle.getValue();
        String operationRef = operHandle.getValue();
        BOHandle sfcHandle = new SFCBOHandle(site, sfc);  //SFC
        singoff.setSfcRef(sfcHandle.getValue());
        singoff.setOperationRef(operationRef);
        singoff.setResourceRef(resourceRef);
        sfcData.add(singoff);
        signoffRequest.setSfcData(sfcData);
        try {
            signoffs.signoffSfc(signoffRequest);
        } catch (Exception ex) {
            String errinf = site+","+userid+","+operation+","+resource+","+sfc+":"+ex.toString();
            SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, Location.getLocation(SfcOperationService.class), "Signoff", errinf);
            ret = "E:"+ex.getMessage();
        }
        return ret;
    }
    /**
     * 完成SFC
     * @param [types] resource:资源; operation:操作
     * @return String 
     * @throws  
     */ 
    public String QuickComplete(String site, String userid, String operation, String resource, String sfc) throws BusinessException {
        String returnInf="S";
//        if (!this.isInQueue(site, operation, sfc)) {
//            return "此SFC不在排队状态，不能开始！";
//        }
        SfcCompleteServiceInterface sfcCompleteService = Services.getService(SFC_PACKAGE, SFC_COMPLETE_SERVICE, site);
        CompleteSfcQuickRequest sfcreq = new CompleteSfcQuickRequest();     
        BOHandle operHandle = new OperationBOHandle(site, operation, "#");  //操作
        BOHandle resourceHandle = new ResourceBOHandle(site, resource);  //资源
        String resourceRef = resourceHandle.getValue();
        String operationRef = operHandle.getValue();
        BOHandle sfcHandle = new SFCBOHandle(site, sfc);  //SFC

        sfcreq.setSfcRef(sfcHandle.getValue());
        sfcreq.setOperationRef(operationRef);
        sfcreq.setResourceRef(resourceRef);

//        try {
//        if (this.isInCompleted(site, operation, sfc)) {
        if (this.isInQueue(site, operation, sfc)) {
        	sfcCompleteService.completeSfcQuick(sfcreq);
        }
//        } catch (Exception e) {
//            String errinf = site+","+userid+","+operation+","+resource+","+sfc+":"+e.toString();
//            SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, Location.getLocation(SfcOperationService.class), "Complete", errinf);
//            Data data = new Data();
//            data.put("QUICK_COMPLETE_EXCEPTION", e.getMessage());
//            throw new BasicBOBeanException(20504, e, data);
//        } 
        return returnInf;
        
    }
    //创建SFC
    public CreateSfcResponse CreateSfc(String site, String res, String item, String oper, String sfc, double qty) throws BusinessException {
        String ret = "E";
        BOHandle operHandle = new OperationBOHandle(site, oper, "#");//操作
        BOHandle resourceHandle = new ResourceBOHandle(site, res);//资源
        BOHandle itemHandle = new ItemBOHandle(site, item, "#");//物料
        
        String resref = resourceHandle.getValue();
        String itemref = itemHandle.getValue();
        String operationRef = operHandle.getValue();
        
        CreateSfcServiceInterface service = Services.getService(SFC_PACKAGE, "CreateSfcService", site);
        CreateSfcRequest request = new CreateSfcRequest();
        request.setItemRef(itemref);
        request.setOperationRef(operationRef);
        request.setResourceRef(resref);
        request.setSfc(sfc);
        request.setQuantity(BigDecimal.valueOf(qty));
        
        return service.createSfc(request);
    }   
    //判断SFC是否在工序上已完成
    public boolean isInCompleted(String site, String operation, String sfc){
        boolean ret = false;
        SfcStateServiceInterface stateService = Services.getService(SFC_PACKAGE, STATESERVICE, site);
        ValidateSfcAtOperationRequest sfcoprequest = new  ValidateSfcAtOperationRequest();
        sfcoprequest.setOperationRef("OperationBO:"+site+","+operation+",#");
        sfcoprequest.setSfcRef("SFCBO:"+site+","+sfc);
        try {
            ret = stateService.isSfcCompletedAtOperation(sfcoprequest);
        } catch (BusinessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return ret;
    }
    //判断SFC是否在工序上排队
    public boolean isInQueue(String site, String operation, String sfc){
        boolean ret = false;
        SfcStateServiceInterface stateService = Services.getService(SFC_PACKAGE, STATESERVICE, site);
        ValidateSfcAtOperationRequest sfcoprequest = new  ValidateSfcAtOperationRequest();
        sfcoprequest.setOperationRef("OperationBO:"+site+","+operation+",#");
        sfcoprequest.setSfcRef("SFCBO:"+site+","+sfc);
        try {
            ret = stateService.isSfcInQueueAtOperation(sfcoprequest);
        } catch (BusinessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return ret;
    }
    //判断SFC是否在工序上是工作状态
    public boolean isInWork(String site, String operation, String sfc){
        boolean ret = false;
        SfcStateServiceInterface stateService = Services.getService(SFC_PACKAGE, STATESERVICE, site);
        ValidateSfcAtOperationRequest sfcoprequest = new  ValidateSfcAtOperationRequest();
        sfcoprequest.setOperationRef("OperationBO:"+site+","+operation+",#");
        sfcoprequest.setSfcRef("SFCBO:"+site+","+sfc);
        try {
            ret = stateService.isSfcInWorkOperation(sfcoprequest);
        } catch (BusinessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return ret;
    }
    public String CheckSfcStatus(String site, String operation, String resource, String sfc){
        SfcStateServiceInterface stateService = Services.getService(SFC_PACKAGE, STATESERVICE, site);
        String returns = "I";
        String statusref = "";
        try {
            if (!this.CheckSfcExist(site, sfc).equals("I")) {
                return "E:[" + sfc + "]不存在";
            }
            SfcBasicData sfcdb = stateService.readSfc("SFCBO:"+site+","+sfc);
            statusref = sfcdb.getStatusRef();
            if (!(statusref.contains("401")||statusref.contains("402"))) {
                return "E:["+sfc+"]已经下线,不能在该工序["+operation+"]加工处理";
            }
            Collection<SfcStep> sfcstepc = stateService.findCurrentRouterSfcStepsBySfcRef(new ObjectReference("SFCBO:"+site+","+sfc));
            for (Iterator<SfcStep> sfcs = sfcstepc.iterator(); sfcs.hasNext();) {
                SfcStep sfcstep = sfcs.next();
                if (sfcstep.getStatus() == SfcStepStatusEnum.IN_QUEUE && (statusref.contains("402")||statusref.contains("401"))) {
                    if (sfcstep.getOperationRef().contains(","+operation+",#")) {
                        returns = "I:["+sfc+"]状态检查符合";
                    } else {
                        String oper = sfcstep.getOperationRef();
                        oper = oper.substring(oper.indexOf(",")+1);
                        oper = oper.substring(0, oper.indexOf(","));
                        returns = "E:["+sfc+"]在["+oper+"]工序中["+sfcstep.getStatus()+"],不能在该工序["+operation+"]加工处理";
                    }
                    break;
                } 
            }
        } catch (Exception e) { 
            returns = "E:"+e.getMessage();
        }
        return returns;
    }   

    /**
     * 检查SFC是否存在
     * @param [types] resource:资源; operation:操作
     * @return String 
     * @throws  
     */ 
    public String CheckSfcExist(String site, String sfc){
        SfcStateServiceInterface stateService = Services.getService(SFC_PACKAGE, STATESERVICE, site);
        FindSfcByNameRequest request = new FindSfcByNameRequest();
        request.setSfc(sfc);
        String returns="E";
        try {
            SfcBasicData sfcbasicdata = stateService.findSfcByName(request);
            if(sfcbasicdata == null){
                returns ="E:[" + sfc + "]不存在";
            }else{
                returns = "I";
            }
        } catch (BusinessException e) {
             returns ="E:[" + sfc + "]异常";
        }
        return returns;
    }
    //检测SFC状态，参数status开始时传402，完成时传403
    public String CheckSfcStatus(String site, String operation, String resource, String sfc, String status) {
        SfcStateServiceInterface stateService = Services.getService(SFC_PACKAGE, STATESERVICE, site);
        //BOHandle sfcHandle = new SFCBOHandle(site, sfc);//SFC
        String returns = "I";
        String statusref = "";
        try {
            if (!this.CheckSfcExist(site, sfc).equals("I")) {
                return "E:[" + sfc + "]不存在";
            }
            SfcBasicData sfcdb = stateService.readSfc("SFCBO:"+site+","+sfc);
            statusref = sfcdb.getStatusRef();
            if (!(statusref.contains("401")||statusref.contains("402")||statusref.contains("403"))) {
                return "E:["+sfc+"]已经下线,不能在该工序["+operation+"]加工处理";
            }
            Collection<SfcStep> sfcstepc = stateService.findCurrentRouterSfcStepsBySfcRef(new ObjectReference("SFCBO:"+site+","+sfc));
            for (Iterator<SfcStep> sfcs = sfcstepc.iterator(); sfcs.hasNext();) {
                SfcStep sfcstep = sfcs.next();
                if (sfcstep.getStatus() == SfcStepStatusEnum.IN_QUEUE && (statusref.contains("402")||statusref.contains("401"))) {
                    if (sfcstep.getOperationRef().contains(","+operation+",#")) {
                        returns = "I:["+sfc+"]状态检查符合";
                    } else {
                        String oper = sfcstep.getOperationRef();
                        oper = oper.substring(oper.indexOf(",")+1);
                        oper = oper.substring(0, oper.indexOf(","));
                        returns = "E:["+sfc+"]在["+oper+"]工序中,不能在该工序["+operation+"]加工处理";
                    }
                    break;
                } else if (statusref.contains("403")) {
                    if (sfcstep.getStatus() == SfcStepStatusEnum.IN_WORK) {
                        String sfcstepref = sfcstep.getSfcStepRef();
                        String sql="select t.resource_bo from sfc_in_work t where t.sfc_step_bo='"+sfcstepref+"'";  //获取Resource
                        LogicService lservice = new LogicService();
                        SqlQueryRequest sqlrequest = new SqlQueryRequest();
                        sqlrequest.setSql(sql);
                        LogicQueryResponse response = lservice.query(sqlrequest);
                        List<Map> resList = response.getRecords();
                        String resourcebo="";
                        if(resList!=null&&resList.size()>0)
                        {
                            resourcebo = resList.get(0).get("RESOURCE_BO").toString();
                        }

                        if (resourcebo.substring(resourcebo.indexOf(",")+1).equals(resource) && sfcstep.getOperationRef().contains(","+operation+",#")) {
                            if (status.equals("402")) {
                                returns = "I:["+sfc+"]已经在该工序处理中";
                            } else {
                                returns = "I:["+sfc+"]状态检查符合";
                            }
                        } else {
                            String oper = sfcstep.getOperationRef();
                            oper = oper.substring(oper.indexOf(",")+1);
                            oper = oper.substring(0, oper.indexOf(","));
                            returns = "E:["+sfc+"]在["+oper+"]工序中,不能在该工序["+operation+"]加工处理";
                        }
                        break;
                    }
                }
            }
        } catch (Exception e) { 
            SimpleLogger.log(Severity.INFO, Category.SYS_SERVER, Location.getLocation(SfcOperationService.class), "start", site+","+operation+","+resource+","+sfc);
            returns = "E:"+e.getMessage();
            
        }
        return returns;
    }
    //生成新SFC号
    public String newSfcCode(String site, String item, String revision, double qty) {
        String code="";
        NumberingServiceInterface service = Services.getService("com.sap.me.numbering", "NumberingService", site);

        GenerateNextNumberRequest numReq = new GenerateNextNumberRequest();
        numReq.setNextNumberType(NextNumberTypeEnum.SFCRELEASE);
        numReq.setItemRef("ItemBO:"+site+","+item+","+revision);
        numReq.setShopOrderTypeRef("ShopOrderTypeBO:"+site+",PRODUCTION");
        numReq.setNumberOfValues(new BigDecimal(qty));
        try {
            GenerateNextNumberResponse numResp = service.generateNextNumber(numReq);
            List<String> idlist = numResp.getId();
            for (String id : idlist) {
                if (code.equals("")) {
                    code = id;
                } else {
                    code = code + "," + id;
                }
            }
        } catch (BusinessException e) {
            SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, Location.getLocation(SfcOperationService.class), "newSfcCode", e.getMessage());
            e.printStackTrace();
        }
        return code;
    }
    //根据工序获得工作指令数据
    public List<Map> getWorkInstruByOP(String site, String operation) throws BusinessException {
        AttachmentConfigurationServiceInterface attachmentService =
           Services.getService("com.sap.me.productdefinition", "AttachmentConfigurationService", site);
        FindAttachmentByAttachedToContextRequest request = new FindAttachmentByAttachedToContextRequest();
        WorkInstructionConfigurationServiceInterface wicService = 
            Services.getService("com.sap.me.productdefinition", "WorkInstructionConfigurationService", site);
        
        AttachedToContext attached = new AttachedToContext();
        BOHandle operHandle = new OperationBOHandle(site, operation, "#");  //操作        
        attached.setOperationRef(operHandle.getValue());
        request.setAttachedToContext(attached);
        request.setAttachmentType(AttachmentType.WORKINSTRUCTION);
        FindAttachmentByAttachedToContextResponse attachmentResponse = new FindAttachmentByAttachedToContextResponse();
        try {
            attachmentResponse= attachmentService.findAttachmentByAttacheds(request );
        } catch (BusinessException e) {
            SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, Location.getLocation(SfcOperationService.class), "getWorkInstruction", e.getMessage());
        }
        List<Map> wiList = new ArrayList<Map>();
        if (attachmentResponse != null && attachmentResponse.getFoundReferencesResponseList()!=null) {
            List<FoundReferencesResponse> responseList = attachmentResponse.getFoundReferencesResponseList();
            for (FoundReferencesResponse response : responseList) {
                for (String ref : response.getRefList()) {
                    WorkInstructionFullConfiguration wifconf = new WorkInstructionFullConfiguration();
                    try {
                        wifconf = wicService.readWorkInstruction(new ObjectReference(ref));
                    } catch (WorkInstructionNotFoundException e) {
                        SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, Location.getLocation(SfcOperationService.class), "getWorkInstruction", e.getMessage());
                        throw new ExtBusinessException(this.getValue(ref)+"工作指令没有找到！");
                    } catch (BusinessException e) {
                        SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, Location.getLocation(SfcOperationService.class), "getWorkInstruction", e.getMessage());
                        throw new ExtBusinessException("查询工作指令"+this.getValue(ref)+"出现错误："+e.getMessage());
                    }
                    Map<String, Object> map = new HashMap<String, Object>();
                    map.put("workinstruction", wifconf.getWorkInstruction());
                    map.put("description", wifconf.getDescription());
                    map.put("url", wifconf.getUrl());
                    map.put("data", wifconf.getInstructionData());
                    map.put("type", wifconf.getSimpleInstruction()); //true表示为文本，否则为url
                    wiList.add(map);
                }
            }
        }
        return wiList;
    }
    //根据工单获得工作指令数据
    public List<Map> getWorkInstruBySO(String site, String shoporder) throws BusinessException {
        AttachmentConfigurationServiceInterface attachmentService =
           Services.getService("com.sap.me.productdefinition", "AttachmentConfigurationService", site);
        FindAttachmentByAttachedToContextRequest request = new FindAttachmentByAttachedToContextRequest();
        WorkInstructionConfigurationServiceInterface wicService = 
            Services.getService("com.sap.me.productdefinition", "WorkInstructionConfigurationService", site);
        
        AttachedToContext attached = new AttachedToContext();
        String soref = new ShopOrderBOHandle(site, shoporder).getValue();  //工单handle       
        attached.setShopOrderRef(soref);
        request.setAttachedToContext(attached);
        request.setAttachmentType(AttachmentType.WORKINSTRUCTION);
        FindAttachmentByAttachedToContextResponse attachmentResponse = new FindAttachmentByAttachedToContextResponse();
        try {
            attachmentResponse= attachmentService.findAttachmentByAttacheds(request );
        } catch (BusinessException e) {
            SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, Location.getLocation(SfcOperationService.class), "getWorkInstruBySO", e.getMessage());
        }
        List<Map> wiList = new ArrayList<Map>();
        if (attachmentResponse != null && attachmentResponse.getFoundReferencesResponseList()!=null) {
            List<FoundReferencesResponse> responseList = attachmentResponse.getFoundReferencesResponseList();
            for (FoundReferencesResponse response : responseList) {
                for (String ref : response.getRefList()) {
                    WorkInstructionFullConfiguration wifconf = new WorkInstructionFullConfiguration();
                    try {
                        wifconf = wicService.readWorkInstruction(new ObjectReference(ref));
                    } catch (WorkInstructionNotFoundException e) {
                        SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, Location.getLocation(SfcOperationService.class), "getWorkInstruction", e.getMessage());
                        throw new ExtBusinessException(this.getValue(ref)+"工作指令没有找到！");
                    } catch (BusinessException e) {
                        SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, Location.getLocation(SfcOperationService.class), "getWorkInstruction", e.getMessage());
                        throw new ExtBusinessException("查询工作指令"+this.getValue(ref)+"出现错误："+e.getMessage());
                    }
                    Map<String, Object> map = new HashMap<String, Object>();
                    map.put("workinstruction", wifconf.getWorkInstruction());
                    map.put("description", wifconf.getDescription());
                    map.put("url", wifconf.getUrl());
                    map.put("data", wifconf.getInstructionData());
                    map.put("type", wifconf.getSimpleInstruction()); //true表示为文本，否则为url
                    wiList.add(map);
                }
            }
        }
        return wiList;
    }   
    //SFC装配组件
    public AssembleComponentsResponse AssembleCom(SfcOperationRequest sfcrequest, String secondOperationV) throws BusinessException, Exception {
        String site = sfcrequest.getSite();
        String sfc = sfcrequest.getSfc();
        String res = sfcrequest.getResource();
        String oper = sfcrequest.getOperation(); 
        String[] attri = sfcrequest.getAttri();
        String[] val = sfcrequest.getVal();
        double qty = sfcrequest.getQty();
        String invid = sfcrequest.getInvid();
        String refdes = sfcrequest.getRefdes();
        String itemRef = sfcrequest.getItemRef();
        
        String ret = "S";
        AssemblyServiceInterface service = Services.getService("com.sap.me.production", "AssemblyService", site);
        AssembleComponentsRequest request = new AssembleComponentsRequest();
        request.setEvent("baseFinished:AssemblyPoint");
        String oref = "OperationBO:"+site+","+oper+",#";
        
        OperationConfigurationServiceInterface operationConfigurationService = Services.getService("com.sap.me.productdefinition", "OperationConfigurationService", site);
        FindOperationCurrentRevisionRequest operationRequest = new FindOperationCurrentRevisionRequest();
        operationRequest.setOperation(oper);
        String opRef = operationConfigurationService.findOperationCurrentRevision(operationRequest).getRef();
        request.setOperationRef(opRef);
        request.setResourceRef("ResourceBO:"+site+","+res);
        String sfcref = "SFCBO:" + site + "," + sfc;
        request.setSfcRef(sfcref);
        List<AssemblyComponent> componentList = new ArrayList<AssemblyComponent>();
        AssemblyComponent component = new AssemblyComponent();
        
        // 查询BOM和物料信息;
        Map<String, Object> queryParams = new HashMap<String, Object>();
        if (StringUtils.isNotBlank(secondOperationV)) {
        	queryParams.put("OPERATION_BO", "OperationBO:" + site + "," + secondOperationV + ",#");
        } else {
            queryParams.put("OPERATION_BO", oref);
        }
        queryParams.put("SFC_BO", sfcref);
        queryParams.put("ITEM_BO", itemRef);
        DataCoreServiceInterface dataCoreService = Services.getService("com.sapdev.service", "DataCoreService", site);
        List<Map<String, Object>> data = dataCoreService.getResultByTCODE("YC_020_08_GET_BOM_COMP", queryParams); 
        String bomCompRef = "";
        String compRef = "";
        if (data != null && data.size() > 0) {
            bomCompRef = data.get(0).get("HANDLE").toString();
            compRef = data.get(0).get("COMPONENT_GBO").toString();
        }
        if (refdes != null) {
           component.setRefDes(refdes);
        }
        component.setResolvedFromIdentifier(invid);
        component.setBomComponentRef(bomCompRef);
        component.setActualComponentRef(compRef);
        component.setQty(BigDecimal.valueOf(qty));
        List<AssemblyDataField> assemblyDataFields = new ArrayList<AssemblyDataField>();
        for (int i=0; i<val.length; i++) {
            AssemblyDataField adf = new AssemblyDataField();
            adf.setSequence(BigDecimal.valueOf(i+1));
            adf.setAttribute(attri[i]);
            adf.setValue(val[i]);
            assemblyDataFields.add(adf);
        }
        component.setAssemblyDataFields(assemblyDataFields);
        componentList.add(component);
        
        request.setComponentList(componentList);
        return service.assembleComponents(request);
    }
    
    //SFC装配组件
    public AssembleComponentsResponse assembleComNonBom(SfcOperationRequest sfcrequest) throws BusinessException, Exception {
        String site = sfcrequest.getSite();
        String sfc = sfcrequest.getSfc();
        String res = sfcrequest.getResource();
        String oper = sfcrequest.getOperation(); 
        String[] attri = sfcrequest.getAttri();
        String[] val = sfcrequest.getVal();
        double qty = sfcrequest.getQty();
        String invid = sfcrequest.getInvid();
        String refdes = sfcrequest.getRefdes();
        String itemRef = sfcrequest.getItemRef();
        
//        String ret = "S";
        AssemblyServiceInterface service = Services.getService("com.sap.me.production", "AssemblyService", site);
//        AssembleComponentsRequest request = new AssembleComponentsRequest();
        AssembleNonBomComponentsRequest request = new AssembleNonBomComponentsRequest();
        request.setEvent("baseFinished:AssemblyPoint");
//        String oref = "OperationBO:"+site+","+oper+",#";
        
        OperationConfigurationServiceInterface operationConfigurationService = Services.getService("com.sap.me.productdefinition", "OperationConfigurationService", site);
        FindOperationCurrentRevisionRequest operationRequest = new FindOperationCurrentRevisionRequest();
        operationRequest.setOperation(oper);
        String opRef = operationConfigurationService.findOperationCurrentRevision(operationRequest).getRef();
        request.setOperationRef(opRef);
        request.setResourceRef("ResourceBO:"+site+","+res);
        String sfcref = "SFCBO:" + site + "," + sfc;
        request.setSfcRef(sfcref);
        List<AssemblyComponent> componentList = new ArrayList<AssemblyComponent>();
        AssemblyComponent component = new AssemblyComponent();
        
        // 查询BOM和物料信息;
//        Map<String, Object> queryParams = new HashMap<String, Object>();
//        queryParams.put("OPERATION_BO", oref);
//        queryParams.put("SFC_BO", sfcref);
//        queryParams.put("ITEM_BO", itemRef);
//        DataCoreServiceInterface dataCoreService = Services.getService("com.sapdev.service", "DataCoreService", site);
//        List<Map<String, Object>> data = dataCoreService.getResultByTCODE("YC_020_08_GET_BOM_COMP", queryParams); 
//        String bomCompRef = "";
//        String compRef = "";
//        if (data != null && data.size() > 0) {
//            bomCompRef = data.get(0).get("HANDLE").toString();
//            compRef = data.get(0).get("COMPONENT_GBO").toString();
//        }
        if (refdes != null) {
           component.setRefDes(refdes);
        }
        component.setResolvedFromIdentifier(invid);
//        component.setBomComponentRef(bomCompRef);
        component.setActualComponentRef(itemRef);
        component.setQty(BigDecimal.valueOf(qty));
        List<AssemblyDataField> assemblyDataFields = new ArrayList<AssemblyDataField>();
        for (int i=0; i<val.length; i++) {
            AssemblyDataField adf = new AssemblyDataField();
            adf.setSequence(BigDecimal.valueOf(i+1));
            adf.setAttribute(attri[i]);
            adf.setValue(val[i]);
            assemblyDataFields.add(adf);
        }
        component.setAssemblyDataFields(assemblyDataFields);
        componentList.add(component);
        
        request.setComponentList(componentList);
        return service.assembleNonBomComponents(request);
    }
    
    /**
     * 获取数据收集数据列表
     * @param [param] 资源BO或工作中心BO
     * @param [types] resource:资源; workcenter:工作中心
     * @return List<DCData> [包装内容列表]
     */ 
    public List<SfcOperationResponse> getDCData(String param, String types, String site){
        List<SfcOperationResponse> dcDatas = new ArrayList<SfcOperationResponse>();
        DataCollectionServiceInterface service = Services.getService("com.sap.me.datacollection", "DataCollectionService", site);
        if (types.equals("resource")) {
            DcGroupsForResourceRequest request = new DcGroupsForResourceRequest();
            request.setResourceRef(param);
            try {
                DCGroupsToCollectResponse response = service.findDcGroupsForResource(request);
                List<DCGroupToCollectResponse> dclist = response.getDcGroupList();
                for (Iterator<DCGroupToCollectResponse> dcres = dclist.iterator(); dcres.hasNext();) {
                    DCGroupToCollectResponse dcre = dcres.next();
                    SfcOperationResponse dcdata = new SfcOperationResponse();
                    dcdata.setDcgroup(dcre.getDcGroup());
                    dcdata.setDcgroupref(dcre.getDcGroupRef());
                    dcdata.setResource(this.getValue(param));
                    List<DCParameterResponse> dcparams = dcre.getDcParameterList();
                    List<DCData> dcdatax = new ArrayList<DCData>();
                    for (Iterator<DCParameterResponse> dcps = dcparams.iterator(); dcps.hasNext();) {
                        DCParameterResponse dcp = dcps.next();
                        DCData dcdatas = new DCData();
                        dcdatas.setDatatype(dcp.getDataType().toString());
                        dcdatas.setMinvalue(dcp.getMinValue()==null?0:dcp.getMinValue().doubleValue());
                        dcdatas.setMaxvalue(dcp.getMaxValue()==null?0:dcp.getMaxValue().doubleValue());
                        dcdatas.setPrompt(dcp.getPrompt());
                        dcdatas.setUnits(dcp.getUnits());
                        dcdatas.setParamter(dcp.getParameterName());
                        dcdatas.setMissing(dcp.isAllowMissingValue());
                        dcdatas.setMinmax(dcp.isOverrideMinMax());
                        if (dcp.getValueMask()!=null) {
                               dcdatas.setValuemask(dcp.getValueMask());
                        }
                        
                        dcdatax.add(dcdatas);
                    }
                    dcdata.setDcdata(dcdatax);
                    dcDatas.add(dcdata);
                }
            } catch (BusinessException e) {
                SimpleLogger.log(Severity.ERROR, Category.APPS_COMMON_FAILOVER, Location.getLocation(SfcOperationService.class), "SfcOperationService.getDCData", e.toString());
            }
        } else {
            DcGroupsForWorkCenterRequest request = new DcGroupsForWorkCenterRequest();
            request.setWorkCenterRef(param);
            try {
                DCGroupsToCollectResponse response = service.findDcGroupsForWorkCenter(request);
                List<DCGroupToCollectResponse> dclist = response.getDcGroupList();
                for (Iterator<DCGroupToCollectResponse> dcres = dclist.iterator(); dcres.hasNext();) {
                    DCGroupToCollectResponse dcre = dcres.next();
                    SfcOperationResponse dcdata = new SfcOperationResponse();
                    dcdata.setDcgroup(dcre.getDcGroup()); 
                    dcdata.setDcgroupref(dcre.getDcGroupRef());
                    dcdata.setResource(this.getValue(param));
                    List<DCParameterResponse> dcparams = dcre.getDcParameterList();
                    List<DCData> dcdatax = new ArrayList<DCData>();
                    for (Iterator<DCParameterResponse> dcps = dcparams.iterator(); dcps.hasNext();) {
                        DCParameterResponse dcp = dcps.next();
                        
                        DCData dcdatas = new DCData();
                        dcdatas.setDatatype(dcp.getDataType().toString());
                        dcdatas.setMinvalue(dcp.getMinValue()==null?0:dcp.getMinValue().doubleValue());
                        dcdatas.setMaxvalue(dcp.getMaxValue()==null?0:dcp.getMaxValue().doubleValue());
                        dcdatas.setPrompt(dcp.getPrompt());
                        dcdatas.setUnits(dcp.getUnits());
                        dcdatas.setParamter(dcp.getParameterName());
                        dcdatas.setMissing(dcp.isAllowMissingValue());
                        dcdatas.setMinmax(dcp.isOverrideMinMax());
                        if (dcp.getValueMask()!=null) {
                           dcdatas.setValuemask(dcp.getValueMask());
                        }
                        dcdatax.add(dcdatas);
                    }
                    dcdata.setDcdata(dcdatax);
                    dcDatas.add(dcdata);
                }
            } catch (BusinessException e) {
                SimpleLogger.log(Severity.ERROR, Category.APPS_COMMON_FAILOVER, Location.getLocation(SfcOperationService.class), "SfcOperationService.getDCData", e.toString());
            }
        }
        return dcDatas;
    }   
    //数据收集
    public void SaveActual(String[] actual, String[] paramname, String dcgroup, String dcgroupref, String site, String trsid) {
        List<CreateParametricMeasure> pmlist = new ArrayList<CreateParametricMeasure>();
        for (int i=0; i<actual.length; i++) {
            CreateParametricMeasure parMea = new CreateParametricMeasure();
            parMea.setActual(actual[i]);
            parMea.setActualNumber(BigDecimal.valueOf(Long.valueOf(actual[i])));
            //parMea.setDataType(datatype);
            parMea.setMeasureName(paramname[i]);
            parMea.setMeasureGroup(dcgroup);
            parMea.setDcParameterRef("cParameterBO:"+dcgroupref+","+paramname[i]);
            
            pmlist.add(parMea);
        }
        CreateParametricRequest crepReq = new CreateParametricRequest();
        crepReq.setParametricMeasureList(pmlist);
        crepReq.setParametricRef("PA:"+site+","+trsid);
        //crepReq.setResourceRef(value);
        //crepReq.setWorkCenterRef(value);
        crepReq.setDcGroupRef(dcgroupref);
        //crepReq.setGboRef(value);
        DataCollectionServiceInterface service = Services.getService("com.sap.me.datacollection", "DataCollectionService", site);
        try {
            service.createParametricData(crepReq);
        } catch (BusinessException e) {
            SimpleLogger.log(Severity.ERROR, Category.APPS_COMMON_FAILOVER, Location.getLocation(SfcOperationService.class), "DCDataBean.SaveActual", e.toString());
            e.printStackTrace();
        }
    }   
    /**
     * 保存sfc数据
     */
    public void saveSfcData(String site, String sfc, String field, String value) throws BusinessException {
        SfcDataServiceInterface sfcDataService = Services.getService("com.sap.me.production","SfcDataService");
        CollectSfcDataRequest collectSfcData = new CollectSfcDataRequest();
        List<String> sfcList = new ArrayList<String> () ;
        String sfcbo = "SFCBO:"+site+","+sfc;
        sfcList.add(sfcbo);
        collectSfcData.setSfcList(sfcList);
        List<SfcDataField> sfcDataFieldList = new ArrayList<SfcDataField> ();
        SfcDataField sfcDataField = new SfcDataField();
        sfcDataField.setAttribute(field);
        sfcDataField.setValue(value);   
        sfcDataFieldList.add(sfcDataField);
        collectSfcData.setSfcDataFieldList(sfcDataFieldList);
        sfcDataService.collectSfcData(collectSfcData);      
    }
    
    /**
     * 获得sfc数据
     */
    public String getSfcData(String site, String sfc, String field) throws BusinessException {
        String value = "";
        SfcDataServiceInterface sfcDataService = Services.getService("com.sap.me.production","SfcDataService");
        FindSfcDataBySfcRequest request = new FindSfcDataBySfcRequest();
        String sfcbo = "SFCBO:"+site+","+sfc;
        request.setSfcRef(sfcbo);
        FindSfcDataBySfcResponse response = sfcDataService.findSfcDataBySfc(request);
        List<SfcDataField> dataList = response.getSfcDataFieldList();
        for (SfcDataField df : dataList) {
            if (df.getAttribute().equals(field)) {
                value = df.getValue();
                break;
            }
        }

        return value;
    }   
    /**
     * 记录不良
     * @param [types] resource:资源; operation:操作
     * @return String 
     * @throws  
     */ 
    public CreateNCResponse logNC(String site, String userid, String operation, String resource, String sfc, String nccode) throws BusinessException {
//        String returnInf = "E";
        BOHandle resourceHandle = new ResourceBOHandle(site, resource);  //资源
        ProductionContext proCont = new ProductionContext();
        proCont.setResourceRef(resourceHandle.getValue());
        StepIdentifier stepid = new StepIdentifier();
        stepid.setOperationId(operation);
        proCont.setStepIdentifier(stepid);
        
        NCProductionServiceInterface ncService = Services.getService("com.sap.me.nonconformance", "NCProductionService", site);
        CreateNCRequest cNCreq = new CreateNCRequest();
        BOHandle sfcHandle = new SFCBOHandle(site, sfc);  //SFC
        cNCreq.setSfcRef(sfcHandle.getValue());
        BOHandle ncHandle = new NCCodeBOHandle(site, nccode);  //NCCODE
        cNCreq.setNcCodeRef(ncHandle.getValue());       
        cNCreq.setProdCtx(proCont);
        cNCreq.setQty(BigDecimal.valueOf(1L));
        cNCreq.setDateTime(HelperUtil.getCurdate());
        
//        try {
//            CreateNCResponse ncRespon = ncService.createNC(cNCreq);
//            returnInf = "I:不合格记录成功";
//        } catch (BusinessException e) {
//            String errinf = site+","+userid+","+operation+","+resource+","+sfc+","+nccode+":"+e.toString();
//            SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, Location.getLocation(SfcOperationService.class), "LogNCComplete", errinf);
//            returnInf = "E:不合格记录失败";
//        }
        
        return ncService.createNC(cNCreq);       
        
    }
    
    /**
     * @throws BusinessException 
     * 记录不良并转指定工艺路线处理
     * @param [types] resource:资源; operation:操作
     * @return String 
     * @throws  
     */ 
    public String LogNCStandard(String site, String userid, String operation, String resource, String sfc, 
            String nccode, String router, String strversion) throws BusinessException{
        String returnInf = "E";
        String routerRef="";
        if (!router.equals("")) {
            String sql ="";
            if(strversion != null && !strversion.isEmpty() && strversion.equals("#")){
                sql="select handle from router where site='"+site+"' and router='"+router+"' and CURRENT_REVISION='true'";
            }else{
                sql="select handle from router where site='"+site+"' and router='"+router+"' and revision='"+strversion+"'";  //获取ROUTER的REF
            }
            LogicService lservice = new LogicService();
            SqlQueryRequest sqlrequest = new SqlQueryRequest();
            sqlrequest.setSql(sql);
            LogicQueryResponse response = lservice.query(sqlrequest);
            List<Map> data = response.getRecords();
            if(data!=null&&data.size()>0)
            {
                routerRef = data.get(0).get("HANDLE").toString();
            }
        }
        BOHandle resourceHandle = new ResourceBOHandle(site, resource);  //资源
        ProductionContext proCont = new ProductionContext();
        proCont.setResourceRef(resourceHandle.getValue());
        StepIdentifier stepid = new StepIdentifier();
        stepid.setOperationId(operation);
        proCont.setStepIdentifier(stepid);
        
        NCProductionServiceInterface ncService = Services.getService("com.sap.me.nonconformance", "NCProductionService", site);
        CreateNCRequest cNCreq = new CreateNCRequest();
        BOHandle sfcHandle = new SFCBOHandle(site, sfc);  //SFC
        cNCreq.setSfcRef(sfcHandle.getValue());
        BOHandle ncHandle = new NCCodeBOHandle(site, nccode);  //NCCODE
        cNCreq.setNcCodeRef(ncHandle.getValue());       
        cNCreq.setProdCtx(proCont);
        
        DispositionRequest disReq = new DispositionRequest();
        disReq.setNcCodeRef(ncHandle.getValue());  //NCCODE
        disReq.setSfcRef(sfcHandle.getValue());   //SFC 
        
        if (!routerRef.equals("")) {
            DispositionSelection disSel = new DispositionSelection();       
            disSel.setRouterRef(routerRef);
            disReq.setDispositionSelection(disSel);
        }
        disReq.setProdCtx(proCont);
        List<String> ncs = new ArrayList<String>();
        try {
            CreateNCResponse ncRespon = ncService.createNC(cNCreq);
            String nc = ncRespon.getNcRef();
            ncs.add(nc);
            disReq.setNcs(ncs);
            ncService.disposition(disReq);
            //returnInf = Complete(site, userid, operation, revision, resource, sfc);
            returnInf = "I:不合格记录成功,["+sfc+"]等待维修";
        } catch (Exception e) {
            String errinf = site+","+userid+","+operation+","+resource+","+sfc+","+nccode+":"+e.toString();
            SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, Location.getLocation(SfcOperationService.class), "LogNCComplete", errinf);
            returnInf = "E:不合格记录失败";
        }
        
        return returnInf;       
        
    }
    /**
     * @throws BusinessException 
     * 记录不良并完成SFC
     * @param [types] resource:资源; operation:操作
     * @return String 
     * @throws  
     */ 
    public String LogNCComplete(String site, String userid, String operation, String resource, String sfc, 
            String nccode, String router, String strversion) throws BusinessException{
        String returnInf = "E";
        String routerRef="";
        if (!router.equals("")) {
            String sql = "";
            if(strversion != null && !strversion.isEmpty() && strversion.equals("#")){
                sql="select handle from router where site='"+site+"' and router='"+router+"' and CURRENT_REVISION='true'";
            }else{
                sql="select handle from router where site='"+site+"' and router='"+router+"' and revision='"+strversion+"'";  //获取ROUTER的REF
            }
            LogicService lservice = new LogicService();
            SqlQueryRequest sqlrequest = new SqlQueryRequest();
            sqlrequest.setSql(sql);
            LogicQueryResponse response = lservice.query(sqlrequest);
            List<Map> data = response.getRecords();
            if(data!=null&&data.size()>0)
            {
                routerRef = data.get(0).get("HANDLE").toString();
            }
        }
        SfcStartServiceInterface sfcStartService = Services.getService(SFC_PACKAGE, SFC_START_SERVICE, site);
        StartSfcRequest sfcreq = new StartSfcRequest();     
        BOHandle operHandle = new OperationBOHandle(site, operation, "#");//操作
        BOHandle resourceHandle = new ResourceBOHandle(site, resource);//资源
        String resourceRef = resourceHandle.getValue();
        String operationRef = operHandle.getValue();
        BOHandle sfcHandle = new SFCBOHandle(site, sfc);//SFC
        BOHandle userRef = new UserBOHandle(site,userid);
        sfcreq.setSfcRef(sfcHandle.getValue());
        sfcreq.setOperationRef(operationRef);
        sfcreq.setResourceRef(resourceRef);
        sfcreq.setNoSfcValidation(true);
        sfcreq.setUserRef(userRef.getValue());
        List<StartSfcRequest> sfcList = new ArrayList<StartSfcRequest>();
        sfcList.add(sfcreq);

        ProductionContext proCont = new ProductionContext();
        proCont.setResourceRef(resourceHandle.getValue());
        StepIdentifier stepid = new StepIdentifier();
        stepid.setOperationId(operation);
        proCont.setStepIdentifier(stepid);
        
        NCProductionServiceInterface ncService = Services.getService("com.sap.me.nonconformance", "NCProductionService", site);
        CreateNCRequest cNCreq = new CreateNCRequest();
        cNCreq.setSfcRef(sfcHandle.getValue());
        BOHandle ncHandle = new NCCodeBOHandle(site, nccode);  //NCCODE
        cNCreq.setNcCodeRef(ncHandle.getValue());       
        cNCreq.setProdCtx(proCont);
        
        DispositionRequest disReq = new DispositionRequest();
        disReq.setNcCodeRef(ncHandle.getValue());  //NCCODE
        disReq.setSfcRef(sfcHandle.getValue());   //SFC 
        
        if (!routerRef.equals("")) {
            DispositionSelection disSel = new DispositionSelection();       
            disSel.setRouterRef(routerRef);
            disReq.setDispositionSelection(disSel);
        }
        disReq.setProdCtx(proCont);
        List<String> ncs = new ArrayList<String>();
        List<SignoffSfcData> sfcData = new ArrayList<SignoffSfcData>();
        try {
            Collection<StartSfcResponse> sfcColl = sfcStartService.start(sfcList);
            for (StartSfcResponse sfcResp:sfcColl) {
                SignoffSfcData singoff = new SignoffSfcData();
                singoff.setSfcRef(sfcResp.getSfcRef());
                singoff.setOperationRef(operationRef);
                singoff.setResourceRef(resourceRef);
                sfcData.add(singoff);
            }
        } catch (Exception e) {
            String errinf = site+","+userid+","+operation+","+resource+","+sfc+","+nccode+":"+e.toString();
            SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, Location.getLocation(SfcOperationService.class), "start", errinf);
            return "E:不合格记录失败";
        }
        
        try {
            CreateNCResponse ncRespon = ncService.createNC(cNCreq);
            String nc = ncRespon.getNcRef();
            ncs.add(nc);
            disReq.setNcs(ncs);
            ncService.disposition(disReq);

            returnInf = "I:不合格记录成功,["+sfc+"]等待维修";
        } catch (Exception e) {
            String errinf = site+","+userid+","+operation+","+resource+","+sfc+","+nccode+":"+e.toString();
            SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, Location.getLocation(SfcOperationService.class), "LogNCComplete", errinf);
            
            SignoffServiceInterface signoffs = Services.getService(SFC_PACKAGE, "SignoffService", site);
            SignoffRequest signoffRequest = new SignoffRequest();
            signoffRequest.setSfcData(sfcData);
            try {
                signoffs.signoffSfc(signoffRequest);
            } catch (Exception ex) {
                errinf = site+","+userid+","+operation+","+resource+","+sfc+":"+ex.toString();
                SimpleLogger.log(Severity.ERROR, Category.SYS_SERVER, Location.getLocation(SfcOperationService.class), "Signoff", errinf);
            }
            returnInf = "E:不合格记录失败";
        }
        
        return returnInf;       
        
    }
    /**
     * 返回指定工序的自定义数据值
     * @param site
     * @param operation  工序
     * @param field  指定自定义数据字段
     * @return
     * @throws BusinessException
     */
    public String getCustomValueByOper(String site, String operation, String field) throws BusinessException {
        OperationConfigurationServiceInterface service = Services.getService("com.sap.me.productdefinition", "OperationConfigurationService", site);
        FindOperationCurrentRevisionRequest request = new FindOperationCurrentRevisionRequest();
        request.setOperation(operation);
        OperationFullConfiguration ofconf = service.findOperationCurrentRevision(request );
        List<CustomValue> cvList = ofconf.getCustomData();
        for (CustomValue cv : cvList) {
            if (cv.getName().equals(field)) {
                return cv.getValue().toString();
            }
        }
        return null;
    }
    /**
     * 返回指定工单的自定义数据值 如获取工单的ERP主线物料清单O_BOM
     * @param site
     * @param shoporder
     * @param field  如：O_BOM
     * @return
     * @throws ShopOrderInputException
     * @throws ShopOrderNotFoundException
     * @throws BusinessException
     */
    public String getCustomValueBySO(String site, String shoporder, String field) throws BusinessException {
        String shoporderref = "ShopOrderBO:"+site+","+shoporder;
        
        ObjectReference obj = new ObjectReference();
        obj.setRef(shoporderref);
        ShopOrderServiceInterface shopOrderService = Services.getService("com.sap.me.demand","ShopOrderService", site);
        ShopOrderFullConfiguration sofcfg = shopOrderService.readShopOrder(obj);
        
        List<AttributeValue> cdList = sofcfg.getCustomData();
        for (AttributeValue av : cdList) {
            if (av.getAttribute().equals(field)) {
                return av.getValue();
            }
        }
        return null;

    }
    //返回物料自定义数据
    public String getCustomDataOfItem(String site, String item, String field) throws BusinessException {
        ItemConfigurationServiceInterface itemService = Services.getService("com.sap.me.productdefinition", "ItemConfigurationService", site); 
        BOHandle itemHandle = new ItemBOHandle(site, item, "#");//物料
        ItemFullConfiguration itemCF = itemService.readItem(new ObjectReference(itemHandle.getValue()));
        //String itemRef = "";
        for (CustomValue cv :itemCF.getCustomData()) {
            if (cv.getName().equals(field)) {
                return cv.getValue().toString();
            }
        }
        return "";
    }
    /**
     * 获得工序在物料清单中的所有用到的组件物料
     * @param site
     * @param operation
     * @param bomRf
     * @return
     * @throws BusinessException
     */
    public List<Map> getOperCompList(String site, String operation, String bomRf) throws BusinessException {
        List<Map> compList = new ArrayList<Map>();
        BOMConfigurationServiceInterface bomconfService = Services.getService("com.sap.me.productdefinition","BOMConfigurationService", site);
        ItemConfigurationServiceInterface itemService = Services.getService("com.sap.me.productdefinition", "ItemConfigurationService", site);
        ReadBOMRequest readRequest = new ReadBOMRequest();
        readRequest.setBomRef(bomRf);
        BOMFullConfiguration bomfConf = bomconfService.readBOM(readRequest);
        List<BOMComponentConfiguration> bomcompList = bomfConf.getBomComponentList();
        for (BOMComponentConfiguration com : bomcompList) {
            if (operation.equals(this.getValue(com.getOperationRef()))) {
                String itemRef = this.getIteminf(itemService, this.getValue(com.getComponentContext()))[0];
                ItemFullConfiguration ifconf = itemService.readItem(new ObjectReference(itemRef));
                Map<String, Object> comMap = new HashMap<String, Object>();
                comMap.put("item", ifconf.getItem());
                comMap.put("itemdesc", ifconf.getDescription());
                comMap.put("version", ifconf.getRevision());
                comMap.put("qty", com.getQuantity());
                String[] text = new String[2];
                for (CustomValue cv : com.getCustomData()) {
                    if (cv.getName().equals("ZZLIFNR")) {   //供应商
                        comMap.put("supplier", cv.getValue());
                    } else if (cv.getName().equals("POTX1")) {  //装配文本1
                        text[0] = cv.getValue().toString();
                    } else if (cv.getName().equals("POTX2")) {  //装配文本2
                        text[1] = cv.getValue().toString();
                    }
                }
                comMap.put("asstext", text[0]+"\r\n"+text[1]);   //装配文本
                compList.add(comMap);
            }
        }
        return compList;
    }
    /**
     * 执行分组工序操作
     * @param site
     * @param sfc
     * @param operation
     * @param resource
     * @param isstart  true 为sfc开始, 否则为sfc完成
     * @throws BasicBOBeanException 
     * @throws BusinessException 
     * @throws Exception 
     */
    public void execGroupOper(String site, String sfc, String operation, String resource, String userid, boolean isstart) 
            throws BasicBOBeanException, BusinessException, Exception {
    	long var1 = System.currentTimeMillis();
    	
        SfcStateServiceInterface stateService = Services.getService(SFC_PACKAGE, STATESERVICE, site);
        FindSfcByNameRequest nameRequest = new FindSfcByNameRequest();
        nameRequest.setSfc(sfc);
        SfcBasicData sfcData = stateService.findSfcByName(nameRequest );
        if(sfcData == null ){
        	throw new RuntimeException("SfcOperationService.execGroupOper SFC不存在");
        }
        String shoporder = sfcData.getShopOrderRef();
        ObjectReference obj = new ObjectReference();
        obj.setRef(shoporder);
        ShopOrderServiceInterface shopOrderService = Services.getService("com.sap.me.demand","ShopOrderService", site);
        ShopOrderFullConfiguration sofcfg = shopOrderService.readShopOrder(obj);
        List<AttributeValue> cdList = sofcfg.getCustomData();
        String workcenter = this.getCustomField(cdList, "O_PLINE");  //获得工单配置产线
        String item = this.getValue(sofcfg.getPlannedItemRef());
        
        long var2 = System.currentTimeMillis();
        
        OperationConfigurationServiceInterface service = Services.getService("com.sap.me.productdefinition", "OperationConfigurationService", site);
        FindOperationCurrentRevisionRequest request = new FindOperationCurrentRevisionRequest();
        request.setOperation(operation);
        OperationFullConfiguration ofconf = service.findOperationCurrentRevision(request );
        String workcenter2 = this.getValue(ofconf.getErpWorkCenterRef());
        
        List<Map> operList = this.getGroupOper(site, workcenter, item, workcenter2, operation);  //获取分组工序列表
        if (operList == null || operList.size()==0) {  //没有设置分组工序则对当前工序执行相应操作后返回
            if (!isstart) {  
                this.Complete(site, userid, operation, resource, sfc);
                insertSfcInfo(site, userid, operation, resource, sfc);
            } else {
                this.Start(site, userid, operation, resource, sfc);
            }
            return;
        }
        
        long var3 = System.currentTimeMillis();
        
        List<String> list = Arrays.asList(operList.get(0).get("FIELD_SUB").toString().split(","));
        
        long stepListStart = System.currentTimeMillis();
        
        //Collection<SfcStep> stepList = stateService.findCurrentRouterSfcStepsBySfcRef(new ObjectReference("SFCBO:"+site+","+sfc));
       
        long stepListEnd= System.currentTimeMillis();
        
        List<Map> getRouterOper = getRouterOper("SFCBO:"+site+","+sfc);
        
        long getRouterOperEnd= System.currentTimeMillis();
        
        int stepid = 0;
        /*for (SfcStep step : stepList) { //获得当前操作工序的步骤以便下面比较顺序使用
            if (operation.equals(this.getValue(step.getOperationRef()))) {
                stepid = Integer.parseInt(step.getStepId());
                break;
            }
        }*/
        
        for (Map map : getRouterOper) {
        	String opreVal = map.get("OPERATION").toString();
        	String stepIdVal = map.get("STEP_ID").toString();
        	if (operation.equals(opreVal)) {
                stepid = Integer.parseInt(stepIdVal);
                break;
            }
        }
        
        long forGetRouterOperEnd = System.currentTimeMillis();
        
        List<String> groupoper = new ArrayList<String>(list);
        groupoper.add(operation);
        if (getRouterOper.isEmpty() && !isstart) {  //如果是完成操作则先完成当前操作工序
            this.Complete(site, userid, operation, resource, sfc);
        }
        
        long var4 = System.currentTimeMillis();
        DataCoreServiceInterface dataCoreService = Services.getService("com.sapdev.service", "DataCoreService",site);
        for (Map routerMap : getRouterOper) { 
        	String opreVal = routerMap.get("OPERATION").toString();
        	String stepIdVal = routerMap.get("STEP_ID").toString();
        	String doneFlag = routerMap.get("DONE").toString();
        	int stepidParam = Integer.parseInt(stepIdVal);
            String sfcOper =opreVal;
            if (!groupoper.contains(sfcOper)) {
                continue;
            }
            //如果此工序不是正在操作的工序
            if (!operation.equals(sfcOper)) {  
                if (Integer.parseInt(stepIdVal)<stepid && isstart) {  //如果是在开始操作并且此工序步骤在操作工序的前面
                    //如果此工序不在分组工序中
                    if (!groupoper.contains(sfcOper)) {
                        if (!doneFlag.equals("true")) {
                            Data data = new Data();
                            data.put("operation", sfcOper);
                            throw new BasicBOBeanException(20112, data); //sfc在工序sfcOper中未完成
                        }
                        continue;
                    } else {
                        Map map = new HashMap<String, String>();
                        map.put("SITE", site);
                        map.put("OPERATION", sfcOper);
                        List<Map<String, Object>> resourceList = dataCoreService.getResultByTCODE("GET_RES_OPERATION_LOGIC", map);
                        String resourceRef4Dc = resource;
                        if (resourceList!=null && resourceList.size()>0) {
                            resourceRef4Dc = (String)resourceList.get(0).get("RESOURCE_BO");
                            resourceRef4Dc = resourceRef4Dc.replace("ResourceBO:" + site + ",", "");
                        }
                        
                        this.QuickComplete(site, userid, sfcOper, resourceRef4Dc, sfc);
                    }
                } else {
                    if (groupoper.contains(sfcOper)) {
                        Map map = new HashMap<String, String>();
                        map.put("SITE", site);
                        map.put("OPERATION", sfcOper);
                        List<Map<String, Object>> resourceList = dataCoreService.getResultByTCODE("GET_RES_OPERATION_LOGIC", map);
                        String resourceRef4Dc = resource;
                        if (resourceList != null && resourceList.size()>0) {
                            resourceRef4Dc = (String) resourceList.get(0).get("RESOURCE_BO");
                            resourceRef4Dc = resourceRef4Dc.replace("ResourceBO:" + site + ",", "");
                        }
                        
                        if (this.isInQueue(site, sfcOper, sfc)) {
                            this.QuickComplete(site, userid, sfcOper, resourceRef4Dc, sfc);
                        } else if (this.isInWork(site, sfcOper, sfc)) {
                            this.Complete(site, userid, sfcOper, resourceRef4Dc, sfc);
                        }
                    } else {
                        // 如果在当前工序之后, 或者是完成并且不在当前工序组, 则以当前工序进行完成;
                        this.Complete(site, userid, operation, resource, sfc);
                    }
                }
            } else {
                if (isstart) {  //如果是完成操作则先完成当前操作工序
                    this.Start(site, userid, operation, resource, sfc);
                    break;
                } else {
                    if (!this.isInCompleted(site, operation, sfc)) {
                        this.Complete(site, userid, operation, resource, sfc);
                    }
                }
            }
        }
        long var5 = System.currentTimeMillis();
        if (getRouterOper.isEmpty() && isstart) {  //如果是开始操作则执行当前工序的开始操作
            this.Start(site, userid, operation, resource, sfc);
        }
        
        if (!isstart) {
        	insertSfcInfo(site, userid, operation, resource, sfc);
        }
        long var6 = System.currentTimeMillis();
        
        if(var6 - var1 > 1000){
        	SimpleLogger.log(Severity.WARNING, Category.SYS_SERVER, Location.getLocation(SfcOperationService.class), "GroupTimeSpend", String.format("total time: %s, one: %s, two: %s, three: %s, four: %s, five: %s, API: %s, SQL: %s, FOR: %s",
        			new Object[] {
        					String.valueOf(var6-var1),
        					String.valueOf(var2-var1),
        					String.valueOf(var3-var2),
        					String.valueOf(var4-var3),
        					String.valueOf(var5-var4),
        					String.valueOf(var6-var5),
        					String.valueOf(stepListEnd-stepListStart), //原API获取当前发动机对应工艺路线
        					String.valueOf(getRouterOperEnd-stepListEnd), //SQL获取当前发动机对应工艺路线
        					String.valueOf(forGetRouterOperEnd-getRouterOperEnd) //循环遍历
        			}));
        }
    }   
    
    public List<Map>  getRouterOper(String sfcBo) throws Exception{
    	StringBuffer sb = new StringBuffer();
        sb.append("SELECT SUBSTR_BEFORE(SUBSTR_AFTER(SFC_ROUTER_BO,','),',') SFC, ");
        sb.append(" CAST(STEP_ID as INTEGER) STEP_ID, ");
        sb.append(" SUBSTR_BEFORE(SUBSTR_AFTER(OPERATION_BO,','),',') OPERATION, DONE ");
        sb.append(" FROM WIP.SFC_STEP ");
        sb.append("   WHERE SFC_ROUTER_BO LIKE '%" + sfcBo + "%' ");
        sb.append("   ORDER BY STEP_ID ASC ");
        LogicService lservice = new LogicService();
        SqlQueryRequest sqlrequest = new SqlQueryRequest();
        sqlrequest.setSql(sb.toString());
        LogicQueryResponse response = lservice.query(sqlrequest);
        return response.getRecords();
    }
    
    //获得SFC当前工序对应的关键物料组件列表
    public List<String> getComListBySFC(String site, String sfc, String operation) throws BusinessException {
        List<String> comlist = new ArrayList<String>();
        BrowseSfcServiceInterface sfcservice = Services.getService("com.sap.me.browse", "BrowseSfcService", site);
        BrowseSfcRequest request = new BrowseSfcRequest();
        //BOHandle sfcHandle = new SFCBOHandle(site, sfc);//SFC
        request.setSfc(sfc);
        Collection<BrowseSfcResponse> sfclist = sfcservice.browseSfcs(request );
        if (sfclist != null && sfclist.size()>0) {
            for (BrowseSfcResponse response : sfclist) {
                BOMConfigurationServiceInterface bomconfService = Services.getService("com.sap.me.productdefinition","BOMConfigurationService", site);
                ReadBOMRequest readRequest = new ReadBOMRequest();
                readRequest.setBomRef(response.getBomRef());
                BOMFullConfiguration bomfConf = bomconfService.readBOM(readRequest);
                List<BOMComponentConfiguration> bomcompList = bomfConf.getBomComponentList();
                List<Map> operList = this.getGroupOper(site, operation);
                comlist = this.getItemList(site, operList, bomcompList);
            }
        }
        return comlist;
    }   
    //根据工单获得当前工序对应的关键物料组件列表
    public List<String> getComListBySO(String site, String shoporder, String operation) throws BusinessException {
        List<String> comlist = new ArrayList<String>();
        ShopOrderServiceInterface shopOrderService = Services.getService("com.sap.me.demand","ShopOrderService", site);
        String shoporderref = "ShopOrderBO:"+site+","+shoporder;
        
        ObjectReference obj = new ObjectReference();
        obj.setRef(shoporderref);
        ShopOrderFullConfiguration sofcfg = shopOrderService.readShopOrder(obj);
        if (sofcfg != null) {
            BOMConfigurationServiceInterface bomconfService = Services.getService("com.sap.me.productdefinition","BOMConfigurationService", site);
            ReadBOMRequest readRequest = new ReadBOMRequest();
            readRequest.setBomRef(sofcfg.getPlannedBomRef());
            BOMFullConfiguration bomfConf = bomconfService.readBOM(readRequest);
            List<BOMComponentConfiguration> bomcompList = bomfConf.getBomComponentList();
            List<Map> operList = this.getGroupOper(site, operation);
            comlist = this.getItemList(site, operList, bomcompList);
        }
        return comlist;
    }
    //根据工序和BOM组件列表获得物料列表
    private List<String> getItemList(String site, List<Map> operList, List<BOMComponentConfiguration> bomcompList) throws BusinessException {
        List<String> itemList = new ArrayList<String>();
        ItemConfigurationServiceInterface itemService = Services.getService("com.sap.me.productdefinition", "ItemConfigurationService", site);
        int j = 0;
        for (int i=0; i<operList.size(); i++) {
            String operref = operList.get(i).get("FIELD_SUB").toString();
            List<BOMComponentConfiguration> compList = this.getCompConfbyOper(operref, bomcompList);
            
            for (BOMComponentConfiguration compConf : compList) {
                String itemref = this.getIteminf(itemService, this.getValue(compConf.getComponentContext()))[0];
                itemList.add(this.getValue(itemref));
                j++;
            }
        }
        return itemList;
    }
    //根据工序返回BOM中的组件
    private List<BOMComponentConfiguration> getCompConfbyOper(String operref, List<BOMComponentConfiguration> bomcompList) {
        List<BOMComponentConfiguration> comList = new ArrayList<BOMComponentConfiguration>();
        for (BOMComponentConfiguration comp :bomcompList) {
            if (this.getValue(operref).equals(this.getValue(comp.getOperationRef()))) {
                for (CustomValue cv : comp.getCustomData()) {
                    if (cv.getName().equals("KEY_MATERIAL") && (cv.getValue().equals("1") || cv.getValue().equals("2"))) { //关键物料和重要物料标识=“1”or “2”
                        comList.add(comp);
                    }
                }               
            }
        }
        return comList;
    }
    //返回分组工序列表
    private List<Map> getGroupOper(String site, String operation) throws BusinessException {
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT distinct b.field_sub FROM z_group_rule a, z_group_rule_fk b WHERE a.handle=b.parent_handle");
        sb.append(" AND a.site='").append(site).append("'");
        sb.append(" AND a.current_version='TRUE'");
        sb.append(" AND b.field_main='").append(operation).append("'");

        LogicService lservice = new LogicService();
        SqlQueryRequest sqlrequest = new SqlQueryRequest();
        sqlrequest.setSql(sb.toString());
        LogicQueryResponse response = lservice.query(sqlrequest);
        return response.getRecords();

    }   
    //返回分组工序列表
    private List<Map> getGroupOper(String site, String wk1, String item, String wk2, String operation) throws BusinessException {
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT b.field_sub FROM z_group_rule a, z_group_rule_fk b WHERE a.handle=b.parent_handle");
        sb.append(" AND a.site='").append(site).append("'");
        sb.append(" AND a.work_center_1='").append(wk1).append("'");
//      sb.append(" AND a.item='").append(item).append("'");
        sb.append(" AND a.work_center_2='").append(wk2).append("'");
        sb.append(" AND a.current_version='TRUE'");
        sb.append(" AND b.field_main='").append(operation).append("'");

        LogicService lservice = new LogicService();
        SqlQueryRequest sqlrequest = new SqlQueryRequest();
        sqlrequest.setSql(sb.toString());
        LogicQueryResponse response = lservice.query(sqlrequest);
        return response.getRecords();

    }
    //返回当前版本的物料Handle, 描述与版本
    private String[] getIteminf(ItemConfigurationServiceInterface itemService, String item) throws BusinessException {
        String[] iteminf = new String[3];
        ItemSearchRequest itemRequest = new ItemSearchRequest();
        itemRequest.setItem(item);
        ItemSearchResult itemResult = itemService.findItemConfiguration(itemRequest);

        for (ItemBasicConfiguration itembc : itemResult.getItemList()) {
            if (itembc.getCurrentRevision()) {
                iteminf[0] = itembc.getRef();
                iteminf[1] = itembc.getDescription();
                iteminf[2] = itembc.getRevision();
                break;
            }
        }
        return iteminf;
    }

    private String getValue(String ref) {
        if (StringUtils.isBlank(ref)) {
            return null;
        }
        String[] vala = ref.split(",");
        return vala[1];
    }
    //根据工单返回客户自定义数据列表
    private String getCustomField(List<AttributeValue> cdList, String field)  throws BusinessException {    
        
        for (AttributeValue av : cdList) {
            if (av.getAttribute().equals(field)) {
                return av.getValue();
            }
        }
        return null;
    }
    /**
     * 查询工序组
     * @param site
     * @param sfc
     * @param shopOrder
     * @param operation
     * @return
     * @throws Exception
     */
    @Override
    public List<String> getOperationGroup(String site, String sfc, String shopOrder, String operation)
            throws Exception {
        Map<String,Object> paramMap = new HashMap<String,Object>();
        List<String> list = new ArrayList<String>();
        String shopOrderRef = new ShopOrderBOHandle(site, shopOrder).getValue();
        String item = "";
        //查询WORK_CENTER_1  工单的自定义字段：工段 
        if (!Utils.isBlank(shopOrder)) {
            ShopOrderServiceInterface  shopOrderService = Services.getService("com.sap.me.demand", "ShopOrderService", site);
            ShopOrderBasicConfiguration shopOrderBasicConfiguration = shopOrderService.findShopOrder(new ObjectReference(shopOrderRef));
            item = new ItemBOHandle(shopOrderBasicConfiguration.getPlannedItemRef()).getItem();
        }
        //查询WORK_CENTER_1  工单的自定义字段：工段 
        if (Utils.isBlank(shopOrder) && !Utils.isBlank(sfc)) {
            SFCBOHandle sfcBOHandle = new SFCBOHandle(site, sfc);
            SfcStateServiceInterface sfcStateService = Services.getService("com.sap.me.production", "SfcStateService", site);
            SfcBasicData sfcBasicDt = sfcStateService.findSfcDataByRef(new ObjectReference(sfcBOHandle.getValue()));
            shopOrderRef = sfcBasicDt.getShopOrderRef();
            item = new ItemBOHandle(sfcBasicDt.getItemRef()).getItem();
        }
        String work_center_1 = HelperUtil.getCustomData(site, ObjectAliasEnum.SHOP_ORDER, shopOrderRef, "O_PLINE");
        //查询WORK_CENTER_2  工序表中ERP_WORK_CENTER_BO 字段
        String work_center_2 = "";
        if (!Utils.isBlank(operation)) {
            OperationConfigurationServiceInterface operationConfigurationService = Services.getService("com.sap.me.productdefinition", "OperationConfigurationService", site);
            FindOperationCurrentRevisionRequest request = new FindOperationCurrentRevisionRequest();
            request.setOperation(operation);
            OperationFullConfiguration operationFullConfiguration = operationConfigurationService.findOperationCurrentRevision(request);
            work_center_2 = operationFullConfiguration.getErpWorkCenterRef();
        }
        if (!Utils.isBlank(work_center_2)) {
            work_center_2 = new WorkCenterBOHandle(work_center_2).getWorkCenter();
        }
        paramMap.put("WORK_CENTER_1", work_center_1);
        paramMap.put("WORK_CENTER_2", work_center_2);
        paramMap.put("SITE_V", site);
        paramMap.put("ITEM_V", item);
        paramMap.put("OPERATION_V", operation);
        DataCoreServiceInterface dataCoreService = Services.getService("com.sapdev.service", "DataCoreService", site);
        List<Map<String,Object>> list_data = dataCoreService.getResultByTCODE("LOGIC_GET_OPRGROUP", paramMap);
        String operationPre = "OperationBO:"+ site + ",";
        String operationSuf = ",#";
        list.add(operationPre + operation + operationSuf);
        if (list_data != null && list_data.size() > 0) {
        String operationSub = (String)list_data.get(0).get("FIELD_SUB");
        String[] subOperationList = operationSub.split(",");
        for (String str : subOperationList) {
            list.add(operationPre + str + operationSuf);
            }
        }
        return list;
    }
    
    /**
     * 查询工序组
     * @param site
     * @param operation
     * @return
     * @throws Exception
     */
    @Override
    public List<String> getOperationGroup(String site,String operation) throws Exception {
        List<String> list = new ArrayList<String>();
        Map<String, Object> params = new HashMap<String, Object>();
		params.put("operation_v", operation);
		params.put("sit_v", site);
		DataCoreServiceInterface dataCoreService = Services.getService("com.sapdev.service", "DataCoreService", site);
		//判定当前工序是否为主工序，判定逻辑：
		//a)查询分工段号：取【操作面板】界面1区的操作->工作中心维护，查询资源所对应的工作中心编号
		//b)查询产线编号：根据分工段号->工作中心维护,查询分工段号上一层的工作中心编号做为产线编号
		//c)根据 分工段号 + 产线编号 在工序组规则维护 查找出当前版本的分组版本，如果没找到分组版本， 则表示当前工序不是工序组
		//d)根据产线编号 + 分工段号 +  当前分组版本， 到工序组规则维护中查询当前1区工序号是否有维护成主工序
		String _sql = "";
		_sql = "SELECT GR_PK.FIELD_SUB FROM Z_GROUP_RULE_FK GR_PK INNER JOIN  Z_GROUP_RULE GR ON GR.HANDLE = GR_PK.PARENT_HANDLE "
			+ "INNER JOIN (SELECT SUBSTR_AFTER(WC.WORK_CENTER_BO,',') WORK_CENTER_BO, SUBSTR_AFTER(WC.WORK_CENTER_OR_RESOURCE_GBO,',') WORK_CENTER_OR_RESOURCE_GBO " 
            + "FROM OPERATION OP , WORK_CENTER_MEMBER WC WHERE OP.ERP_WORK_CENTER_BO = WC.WORK_CENTER_OR_RESOURCE_GBO "
            + "AND OP.CURRENT_REVISION='true' AND OP.OPERATION =:operation_v AND OP.SITE =:sit_v ) OW "  
            + "ON GR.WORK_CENTER_1=OW.WORK_CENTER_BO AND GR.WORK_CENTER_2= OW.WORK_CENTER_OR_RESOURCE_GBO "
            + "WHERE GR_PK.FIELD_MAIN=:operation_v AND GR.CURRENT_VERSION='TRUE' ";
		
		List<Map<String, Object>> resultListMap	=	dataCoreService.getResultByTSQL(_sql, params)	;
		//是否为工序组，不为空为：工序组
		if (resultListMap != null && !resultListMap.isEmpty()){
			String fieldSub  = resultListMap.get(0).get("FIELD_SUB").toString();//当前操作包含的所有操作
			String[] fieldSubArr =  fieldSub.split(",");//截取出包含的所有操作
			if (fieldSubArr != null){
				for(String item : fieldSubArr){
					list.add(item);
				}
			}
		}
		list.add(operation);	
        return list;
    }
    
    /**
     * 校验SFC是否在当前工序和所有子工序、资源中活动
     * @param site
     * @param operation
     * @return
     * @throws Exception 
     */
    @Override
	public boolean checkSfc(String site, String sfc ,String operation) throws Exception{
		boolean result = true;
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("site_v", site);
		params.put("sfc_v", sfc);
		
		//查询工序组
		List<String>  operationBoList = new ArrayList<String>();
		List<String> operationList = this.getOperationGroup(site, operation);
		for(String op :operationList){
			operationBoList.add("OperationBO:"+site+"," + op);	
		}
		String _sql ="select s.handle "
				    + "from sfc_step sst,sfc_router sr,sfc_routing srt,sfc s,sfc_in_work siw, item i, shop_order so, status st " 
			        + "where sst.sfc_router_bo=sr.handle and srt.handle=sr.sfc_routing_bo and s.handle=srt.sfc_bo " 
				    + "and s.handle=srt.sfc_bo and siw.sfc_step_bo=sst.handle AND replace(s.item_bo, '#', i.revision)=i.handle "
				    + "and i.current_revision='true' AND s.shop_order_bo=so.handle AND s.status_bo=st.handle "
				    + "and s.site=:site_v "  
				    + "and  REPLACE(sst.operation_bo,',#','') in ('" + operationBoList.get(0) + "'";
		
		for(int i = 1; i < operationBoList.size(); i++){
			_sql += (",'" + operationBoList.get(i) + "'");
		}
		_sql += ")";
	
		_sql += (" AND s.sfc=:sfc_v" );
		
		DataCoreServiceInterface dataCoreService = Services.getService("com.sapdev.service", "DataCoreService", site);
		List<Map<String, Object>> list_map = dataCoreService.getResultByTSQL(_sql, params);
		
		if (list_map == null || list_map.isEmpty()){
			result = false;
		}
		return result;
	}
    /**
     * 工单下达
     * @param site
     * @param shopOrderRef
     * @return
     * @throws Exception
     */
    @Override
    public String release(String site, String shopOrderRef) throws Exception{
        String newSfc = null;
        //根据工单查询bom组件数量
        DataCoreServiceInterface dataCoreService = Services.getService("com.sapdev.service", "DataCoreService", site);
        Map<String, Object> _param = new HashMap<String, Object>();
        _param.put("SITE_V", site);
        _param.put("SHOP_ORDER_REF", shopOrderRef);
        List<Map<String, Object>> _list = dataCoreService.getResultByTCODE("LOGIC_GET_ITEM_BOMCOMP_BYORDER", _param);
        double qty = 1;
        if(_list != null && _list.size() > 0){
            qty =  (Double) _list.get(0).get("QTY");
        }
        // 创建SFC号
        ShopOrderServiceInterface shopOrderService = Services.getService("com.sap.me.demand", "ShopOrderService", site);
        ReleaseShopOrderRequest releaseShopOrderRequest = new ReleaseShopOrderRequest();
        releaseShopOrderRequest.setShopOrderRef(shopOrderRef);
        releaseShopOrderRequest.setQuantityToRelease(new BigDecimal(qty));
        ReleaseShopOrderResponse releaseShopOrderResponse = shopOrderService.releaseShopOrder(releaseShopOrderRequest);
        List<ReleasedSfc> releasedSfcs = releaseShopOrderResponse.getReleasedSfcList();
        if (!Utils.isEmpty(releasedSfcs)) {
            ReleasedSfc releasedSfc = releasedSfcs.get(0);
            long startTime = System.currentTimeMillis();
            newSfc = new SFCBOHandle(releasedSfc.getSfcRef()).getSFC();
            long totalTime = System.currentTimeMillis() - startTime;
    		
    		if(totalTime>1000){
    			SimpleLogger.log(Severity.WARNING, category, loc, MESSAGE_ID, 
    					String.format("Start Sfc, total speed time %s, SfcStartServiceInterface, newSfc [%s], shopOrderRef [%s]", 
    							new Object[] {String.valueOf(totalTime), 
    									newSfc, shopOrderRef }));
    		}else{
    			SimpleLogger.log(Severity.INFO, category, loc, MESSAGE_ID, 
    					String.format("Start Sfc, total speed time %s, SfcStartServiceInterface, newSfc [%s], shopOrderRef [%s]", 
    							new Object[] {String.valueOf(totalTime),
    									newSfc, shopOrderRef }));
    		}
        }
        return newSfc;
    }
    /**
     * 工单下达
     * @param site
     * @param shopOrderRef
     * @return
     * @throws Exception
     */
    @Override
    public String releaseOrdBySfc(String site, String shopOrderRef, String sfc) throws BusinessException, Exception {
    	String newSfc = null;
    	//根据工单查询bom组件数量
    	DataCoreServiceInterface dataCoreService = Services.getService("com.sapdev.service", "DataCoreService", site);
    	Map<String, Object> _param = new HashMap<String, Object>();
    	_param.put("SITE_V", site);
    	_param.put("SHOP_ORDER_REF", shopOrderRef);
    	List<Map<String, Object>> _list = dataCoreService.getResultByTCODE("LOGIC_GET_ITEM_BOMCOMP_BYORDER", _param);
    	double qty = 1;
    	if(_list != null && _list.size() > 0){
    		qty =  (Double) _list.get(0).get("QTY");
    	}
    	// 创建SFC号
    	ShopOrderServiceInterface shopOrderService = Services.getService("com.sap.me.demand", "ShopOrderService", site);
    	ReleaseShopOrderRequest releaseShopOrderRequest = new ReleaseShopOrderRequest();
    	releaseShopOrderRequest.setShopOrderRef(shopOrderRef);
    	releaseShopOrderRequest.setQuantityToRelease(new BigDecimal(qty));
    	List<SfcIdentifier> newSfcList = new ArrayList<SfcIdentifier>();
        SfcIdentifier sfcIdentifier = new SfcIdentifier();
        sfcIdentifier.setId(sfc);
        newSfcList.add(sfcIdentifier);
        releaseShopOrderRequest.setNewSfcList(newSfcList);
    	ReleaseShopOrderResponse releaseShopOrderResponse = shopOrderService.releaseShopOrder(releaseShopOrderRequest);
    	List<ReleasedSfc> releasedSfcs = releaseShopOrderResponse.getReleasedSfcList();
    	if (!Utils.isEmpty(releasedSfcs)) {
    		ReleasedSfc releasedSfc = releasedSfcs.get(0);
    		newSfc = new SFCBOHandle(releasedSfc.getSfcRef()).getSFC();
    	}
    	return newSfc;
    }
    /**
     * sfc完成时往插入数据Z_SFC_DATA_L,Z_SFC_DATA
     * @param site
     * @throws Exception
     */
	public void insertSfcInfo(String site, String userid, String operation, String resource, String sfc)
			throws Exception {
		DataCoreServiceInterface dataCoreService = Services.getService("com.sapdev.service", "DataCoreService", site);
		Map<String, Object> _param = new HashMap<String, Object>();
		_param.put("SITE_V", site);
		_param.put("USER_ID", userid);
		_param.put("OPERATION", operation);
		_param.put("RESOURCE", resource);
		_param.put("SFC", sfc);
		dataCoreService.executeByTCODE("INSERT_Z_SFC_DATA", _param);
		List<Map<String, Object>> _list = dataCoreService.getResultByTCODE("GET_USER_GROUP_ID_INFO", _param);
		if (_list != null && _list.size() > 0) {
			Map<String, Object> map = _list.get(0);
			String[] userL = map.get("USER_G").toString().split(";");
			String userId = "";
			for (int i = 0; i < userL.length; i++) {
				userId = userL[i];
				_param.put("USER_L", userId);
				dataCoreService.executeByTCODE("INSERT_Z_SFC_DATA_L", _param);
			}
		}
	}
}

