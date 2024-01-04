<?php

namespace App\Utils\BaseClass\Order;

use Illuminate\Support\Facades\DB;
use App\Utils\AccountHelper;
use App\Utils\BaseClass\Station\Station;
use App\Utils\BaseClass\Station\StationGroup;
use App\Utils\BaseClass\User\User;
use App\Utils\BaseClass\User\UserGroup;
use App\Utils\BaseClass\User\UserGroupUser;
use App\Utils\BaseClass\Device\Device;
use App\Utils\BaseClass\Device\DevicePort;
use App\Utils\BaseClass\Enterprise\Enterprise;
use App\Utils\BaseClass\Order\ChargeOrderSegment;
use App\Http\Controllers\ExcelController;
use App\Services\Order\OccupyOrderService;
use App\Services\Order\OrderService;
use App\Utils\BaseClass\Statistics\Statistics;
use App\Utils\BaseClass\System\SystemUser;
use App\Utils\ChargeHelper;
use App\Utils\FeeHelper;
use App\Utils\NumTransNameHelper as TransHelper;
use log;
use Symfony\Component\Debug\Exception\FatalErrorException;

class ChargeOrder
{
    public function __construct()
    {

    }

    /**
     * 获取订单的统计数据DB（暂未使用）
     * @param $data
     * @return array
     */
    public function getOrderSumDB($data)
    {
        $portId = isset($data['port_id']) ? $data['port_id'] : 0;

        $select = [
            DB::raw('sum(abb_charge_order.payment) as payment'),
            DB::raw('sum(abb_charge_order.duration) as duration'),
            DB::raw('sum(abb_charge_order.elec_quantity) as elecQuantity')
        ];

        $orderInfos = DB::table('charge_order')->select($select)->where('is_del', 0);

        if ($portId) {
            $orderInfos = $orderInfos->where('port_id', $portId);
        }

        $result = $orderInfos->get();

        return $result;
    }

    /**
     * 获取订单列表DB
     * @param $data
     * @return array
     */
    public function getChargeOrderDB($data)
    {
        $offset = isset($data['offset']) ? $data['offset'] : 0;
        $limit = isset($data['limit']) ? $data['limit'] : 20;
        $orderBy = isset($data['orderBy']) ? $data['orderBy'] : 'id';
        $order = isset($data['order']) ? $data['order'] : 'desc';
        $ids = isset($data['ids']) ? $data['ids'] : [];
        $portId = isset($data['portId']) ? $data['portId'] : 0;
        $portIds = isset($data['portIds']) ? $data['portIds'] : [];
        $id = isset($data['id']) ? $data['id'] : 0;

        $orQuery = isset($data['orQuery']) ? $data['orQuery'] : [];
        $stationIds = isset($data['stationIds']) ? $data['stationIds'] : [];
        $deviceIds = isset($data['deviceIds']) ? $data['deviceIds'] : [];
        $status = isset($data['status']) ? $data['status'] : 0;
        $statusArr = isset($data['statusArr']) ? $data['statusArr'] : [];
        $noStatusArr = isset($data['noStatusArr']) ? $data['noStatusArr'] : [];
        $orderNumber = isset($data['orderNumber']) ? $data['orderNumber'] : '';
        $queryOrderNumber = isset($data['queryOrderNumber']) ? $data['queryOrderNumber'] : '';
        $stationId = isset($data['stationId']) ? $data['stationId'] : 0;
        $deviceId = isset($data['deviceId']) ? $data['deviceId'] : 0;
        $userId = isset($data['userId']) ? $data['userId'] : 0;
        $userIds = isset($data['userIds']) ? $data['userIds'] : [];
        $startSTime = isset($data['startSTime']) ? $data['startSTime'] : '';
        $endSTime = isset($data['endSTime']) ? $data['endSTime'] : '';
        $endSt = isset($data['endSt']) ? $data['endSt'] : '';
        $isEndTime = isset($data['isEndTime']) ? $data['isEndTime'] : -1;
        $noOrderTypeArr = isset($data['noOrderTypeArr']) ? $data['noOrderTypeArr'] : [];

        $selectArr = isset($data['select']) ? $data['select'] : [];
        $minDuration = isset($data['minDuration']) ? $data['minDuration'] : 0;
        $minElecQuantity = isset($data['minElecQuantity']) ? $data['minElecQuantity'] : 0;
        $minPayment = isset($data['minPayment']) ? $data['minPayment'] : 0;
        $minElecCostFinal = isset($data['minElecCostFinal']) ? $data['minElecCostFinal'] : 0;
        $minServeCostFinal = isset($data['minServeCostFinal']) ? $data['minServeCostFinal'] : 0;

        $result = [
            'total' => 0,
            'list' => []
        ];

        $select = [
            'id', 'order_number', 'user_id', 'station_id', 'device_id',
            'port_id', 'started_at', 'finished_at', 'elec_cost', 'serve_cost',
            'duration', 'elec_quantity', 'status', 'order_type',
            'charge_finish_reason as chargeFinishReason', 'currency_type', 'currency_ratio',
            'start_type', 'elec_cost_final', 'serve_cost_final', 'vin_code as vinCode',
            'order_status as orderStatus', 'invoice_status as invoiceStatus', 
            'card_number', 'fee_type', 
            'payment as origin_payment',
            'payment_real as payment',
            'serial_number as serialNumber',
            'start_soc as startSoc',
            'end_soc as endSoc',
            'finish_reason_detail as finishReasonDetail'
        ];
        //origin_payment作payment别名，指原价；payment作payment_real别名，指折后价

        if ($selectArr) {
            if (isset($selectArr['sumPayment']) && $selectArr['sumPayment'] == 1) {
                array_push($select, DB::raw('sum(abb_charge_order.payment_real) as sumPayment'));
            }
            if (isset($selectArr['sumElecCostFinal']) && $selectArr['sumElecCostFinal'] == 1) {
                array_push($select, DB::raw('sum(abb_charge_order.elec_cost_final) as sumElecCostFinal'));
            }
            if (isset($selectArr['sumServeCostFinal']) && $selectArr['sumServeCostFinal'] == 1) {
                array_push($select, DB::raw('sum(abb_charge_order.serve_cost_final) as sumServeCostFinal'));
            }
            if (isset($selectArr['sumElecQuantity']) && $selectArr['sumElecQuantity'] == 1) {
                array_push($select, DB::raw('sum(abb_charge_order.elec_quantity) as sumElecQuantity'));
            }
            if (isset($selectArr['sumDuration']) && $selectArr['sumDuration'] == 1) {
                array_push($select, DB::raw('sum(abb_charge_order.duration) as sumDuration'));
            }
        }

        $orderInfos = DB::table('charge_order')->select($select)->where('is_del', 0);

        if ($ids) {
            $orderInfos = $orderInfos->whereIn('id', $ids);
        }

        if (isset($data['id'])) {
            $orderInfos = $orderInfos->where('id', $id);
        }

        if ($portId) {
            $orderInfos = $orderInfos->where('port_id', $portId);
        }

        if (isset($data['portIds'])) {
            $orderInfos = $orderInfos->whereIn('port_id', $portIds);
        }

        if (!empty($orQuery)) {
            $orderInfos = $orderInfos->where(function ($orderInfos) use ($orQuery) {
                if ($orQuery['order_number']) {
                    $orderInfos = $orderInfos->orWhere('order_number', 'like', '%' . $orQuery['order_number'] . '%');
                }
                if ($orQuery['user_id']) {
                    $orderInfos = $orderInfos->orWhereIn('user_id', $orQuery['user_id']);
                }
                if ($orQuery['or_user_id']) {
                    $orderInfos = $orderInfos->orWhereIn('user_id', $orQuery['or_user_id']);
                }
                if ($orQuery['station_id']) {
                    $orderInfos = $orderInfos->orWhereIn('station_id', $orQuery['station_id']);
                }
                if ($orQuery['device_id']) {
                    $orderInfos = $orderInfos->orWhereIn('device_id', $orQuery['device_id']);
                }
            });
        }

        if (isset($data['stationIds'])) {
            $orderInfos = $orderInfos->whereIn('station_id', $stationIds);
        }

        if (!empty($orderNumber)) {
            $orderInfos = $orderInfos->where('order_number', 'like', '%' . $orderNumber . '%');
        }

        if (isset($data['queryOrderNumber'])) {
            $orderInfos = $orderInfos->where('order_number', $queryOrderNumber);
        }

        if ($stationId) {
            $orderInfos = $orderInfos->where('station_id', $stationId);
        }
        
        if (isset($data['deviceIds'])) {
            $orderInfos = $orderInfos->whereIn('device_id', $deviceIds);
        }

        if ($deviceId) {
            $orderInfos = $orderInfos->where('device_id', $deviceId);
        }

        if ($userId) {
            $orderInfos = $orderInfos->where('user_id', $userId);
        }

        if (isset($data['userIds'])) {
            $orderInfos = $orderInfos->whereIn('user_id', $userIds);
        }

        if ($startSTime) {
            $orderInfos = $orderInfos->where('started_at', '>=', $startSTime);
        }

        if ($endSTime) {
            $orderInfos = $orderInfos->where('started_at', '<=', $endSTime);
        }

        if ($endSt) {
            $orderInfos = $orderInfos->where('started_at', '<', $endSt);
        }

        if ($status) {//订单状态
            $orderInfos = $orderInfos->where('status', $status);
        }

        if (isset($data['statusArr'])) {
            $orderInfos = $orderInfos->whereIn('status', $statusArr);
        }

        if (isset($data['noStatusArr'])) {
            $orderInfos = $orderInfos->whereNotIn('status', $noStatusArr);
        }
        
        if (isset($data['noOrderTypeArr'])) {
            $orderInfos = $orderInfos->whereNotIn('order_type', $noOrderTypeArr);
        }
        
        if (isset($data['minDuration'])){
            $orderInfos = $orderInfos->where('duration', '>=', $minDuration);
        }

        if (isset($data['minElecQuantity'])) {
            $orderInfos = $orderInfos->where('elec_quantity', '>=', $minElecQuantity);
        }

        if (isset($data['minPayment'])) {
            $orderInfos = $orderInfos->where('payment', '>=', $minPayment);
        }

        if (isset($data['minElecCostFinal'])) {
            $orderInfos = $orderInfos->where('elec_cost_final', '>=', $minElecCostFinal);
        }

        if (isset($data['minServeCostFinal'])) {
            $orderInfos = $orderInfos->where('serve_cost_final', '>=', $minServeCostFinal);
        }

        if ($isEndTime > 0) {
            if ($isEndTime == 1) {//有结束时间
                $orderInfos = $orderInfos->where('finished_at', '<>', '')->where('finished_at', '<>', null);
            }
        }

        $result['total'] = $orderInfos->count();

        if ($offset >= 0 && $limit > 0) {
            $orderInfos = $orderInfos->skip($offset)->take($limit);
        }

        $result['list'] = $orderInfos->orderBy($orderBy, $order)->get();

        return $result;
    }


    

    public function orderStatics($data, $selectArr) {
        $orderNumber = isset($data['orderNumber'])?$data['orderNumber']:'';
        $phone = isset($data['phone'])?$data['phone']:'';
        $portNumber = isset($data['portNumber'])?$data['portNumber']:'';
        $st = isset($data['st'])?$data['st']:'';
        $et = isset($data['et'])?$data['et']:'';
        $stationId = isset($data['stationId'])?$data['stationId']:0;
        $statusDetail = isset($data['statusArr'])?$data['statusArr']:[];
        $deviceId = isset($data['deviceId'])?$data['deviceId']:0;
        $enterpriseId = isset($data['enterpriseId'])?$data['enterpriseId']:0;
        $authenId = isset($data['authenId'])?$data['authenId']:0;
        $userGroupId = isset($data['userGroupId'])?$data['userGroupId']:0;
        $query = isset($data['query'])?$data['query']:'';
        $select = [];
        if (isset($selectArr['sumPayment']) && $selectArr['sumPayment'] == 1) {
            array_push($select, DB::raw('sum(abb_charge_order.payment_real) as sumPayment'));
        }

        if (isset($selectArr['sumElecQuantity']) && $selectArr['sumElecQuantity'] == 1) {
            array_push($select, DB::raw('sum(case when abb_charge_order.elec_quantity >=0 then abb_charge_order.elec_quantity else 0 end) as sumElecQuantity'));
        }
        if (isset($selectArr['sumDuration']) && $selectArr['sumDuration'] == 1) {
            array_push($select, DB::raw('sum(abb_charge_order.duration) as sumDuration'));
        }

        $sql = DB::table('charge_order')->where('charge_order.is_del', 0);

        if(!empty($enterpriseId)) {
            $enterpriseIds = DB::table('enterprise')->where('is_del', 0)->where(function($build) use ($enterpriseId) {
                $build->where('id', $enterpriseId)->orWhere('parent_id', $enterpriseId);
            })->pluck('id');
            $sql->join('station', function($join) use ($enterpriseIds){
                $join->on('charge_order.station_id','=','station.id')->where('station.is_del',0)->whereIn('station.enterprise_id', $enterpriseIds);
            });    
        }

        if(!empty($orderNumber)) {
            $sql->where('charge_order.order_number', $orderNumber);
        }
        if(!empty($stationId)) {
            $sql->where('charge_order.station_id', $stationId);
        }
        if(!empty($portNumber)) {
            $sql->where('charge_order.port_id', $portNumber);
        }
        if(!empty($st)) {
            $sql->where('charge_order.started_at', '>=', $st);
        }
        if(!empty($et)) {
            $sql->where('charge_order.started_at', '<=', $et);
        }
        if(!empty($statusDetail)) {
            $sql->whereIn('charge_order.status', $statusDetail);
        } else {
            // $sql->whereNotIn('charge_order.status', [7,8,9]);
        }
        if(!empty($authenId)) {
            $sql->where('charge_order.user_id', $authenId);
        }
        if(!empty($deviceId)) {
            $sql->where('charge_order.device_id', $deviceId);
        }
        if(!empty($phone)) {
            $userIds = DB::table('user')->where('is_del', 0)->where('bind_mail', $phone)->pluck('id');
            $sql->whereIn('charge_order.user_id', $userIds);
        }
        if(!empty($userGroupId)) {
            $sql->join('user_group_user', function($join) use ($userGroupId){
                $join->on('charge_order.user_id','=','user_group_user.user_id')->where('user_group_user.is_del',0)->where('user_group_user.group_id', $userGroupId);
            });         
        }
        if(!empty($query)) {
            $orQuery = $this->queryStatistics($data);
            if(!empty($orQuery)) {
                $sql->where(function ($orderInfos) use ($orQuery) {
                    if ($orQuery['order_number']) {
                        $orderInfos = $orderInfos->orWhere('charge_order.order_number', 'like', '%' . $orQuery['order_number'] . '%');
                    }
                    if ($orQuery['user_id']) {
                        $orderInfos = $orderInfos->orWhereIn('charge_order.user_id', $orQuery['user_id']);
                    }
                    if ($orQuery['or_user_id']) {
                        $orderInfos = $orderInfos->orWhereIn('charge_order.user_id', $orQuery['or_user_id']);
                    }
                    if ($orQuery['station_id']) {
                        $orderInfos = $orderInfos->orWhereIn('charge_order.station_id', $orQuery['station_id']);
                    }
                });
            }
        }
        if (isset($selectArr['orderNum']) && $selectArr['orderNum'] == 1) {
            $result = $sql->count();
        } else {
            $result = $sql->select($select)->first();
        }
        return $result;
    }
    
    /**
     * 获取订单列表DB 关联表
     * @param $data
     * @return array
     */
    public function getJoinChargeOrderDB($data)
    {
        $offset = isset($data['offset']) ? $data['offset'] : 0;
        $limit = isset($data['limit']) ? $data['limit'] : 20;
        $orderBy = isset($data['orderBy']) ? $data['orderBy'] : 'id';
        $order = isset($data['order']) ? $data['order'] : 'desc';
        $ids = isset($data['ids']) ? $data['ids'] : [];
        $portId = isset($data['portId']) ? $data['portId'] : 0;
        $portIds = isset($data['portIds']) ? $data['portIds'] : [];
        $id = isset($data['id']) ? $data['id'] : 0;

        $orQuery = isset($data['orQuery']) ? $data['orQuery'] : [];
        $stationIds = isset($data['stationIds']) ? $data['stationIds'] : [];
        $deviceIds = isset($data['deviceIds']) ? $data['deviceIds'] : [];
        $status = isset($data['status']) ? $data['status'] : 0;
        $statusArr = isset($data['statusArr']) ? $data['statusArr'] : [];
        $noStatusArr = isset($data['noStatusArr']) ? $data['noStatusArr'] : [];
        $orderNumber = isset($data['orderNumber']) ? $data['orderNumber'] : '';
        $queryOrderNumber = isset($data['queryOrderNumber']) ? $data['queryOrderNumber'] : '';
        $stationId = isset($data['stationId']) ? $data['stationId'] : 0;
        $deviceId = isset($data['deviceId']) ? $data['deviceId'] : 0;
        $userId = isset($data['userId']) ? $data['userId'] : 0;
        $userIds = isset($data['userIds']) ? $data['userIds'] : [];
        $startSTime = isset($data['startSTime']) ? $data['startSTime'] : '';
        $endSTime = isset($data['endSTime']) ? $data['endSTime'] : '';
        $endSt = isset($data['endSt']) ? $data['endSt'] : '';
        $isEndTime = isset($data['isEndTime']) ? $data['isEndTime'] : -1;
        $noOrderTypeArr = isset($data['noOrderTypeArr']) ? $data['noOrderTypeArr'] : [];

        $selectArr = isset($data['select']) ? $data['select'] : [];
        $minDuration = isset($data['minDuration']) ? $data['minDuration'] : 0;
        $minElecQuantity = isset($data['minElecQuantity']) ? $data['minElecQuantity'] : 0;
        $minPayment = isset($data['minPayment']) ? $data['minPayment'] : 0;
        $minElecCostFinal = isset($data['minElecCostFinal']) ? $data['minElecCostFinal'] : 0;
        $minServeCostFinal = isset($data['minServeCostFinal']) ? $data['minServeCostFinal'] : 0;
        $serialNumber = isset($data['serialNumber']) ? $data['serialNumber'] : '';

        $result = [
            'total' => 0,
            'list' => []
        ];

        $select = [
            'charge_order.id as id',
            'order_number as order_number',
            'user_id as user_id',
            'station_id as station_id',
            'device_id as device_id',
            'port_id as port_id',
            'started_at as started_at',
            'finished_at as finished_at',
            'elec_cost as elec_cost',
            'serve_cost as serve_cost',
            'duration as duration',
            'elec_quantity as elec_quantity',
            'status as status',
            'order_type as order_type',
            'currency_type as currency_type',
            'currency_ratio as currency_ratio',
            'start_type as start_type',
            'elec_cost_final as elec_cost_final',
            'serve_cost_final as serve_cost_final',
            'vin_code as vinCode',
            'order_status as orderStatus',
            'invoice_status as invoiceStatus',
            'card_number as card_number',
            'fee_type as fee_type',
            'payment as origin_payment',
            'payment_real as payment',
            'serial_number as serialNumber',
            'start_soc as startSoc',
            'end_soc as endSoc',
            'finish_reason_detail as finishReasonDetail',
            'charge_finish_reason as chargeFinishReason',
            'charge_order_segment.elec_cost_sharp as elecCostSharp',
            'charge_order_segment.elec_cost_peak as elecCostPeak',
            'charge_order_segment.elec_cost_average as elecCostAverage',
            'charge_order_segment.elec_cost_valley as elecCostValley',
            'charge_order_segment.quantity_sharp as quantitySharp',
            'charge_order_segment.quantity_peak as quantityPeak',
            'charge_order_segment.quantity_average as quantityAverage',
            'charge_order_segment.quantity_valley as quantityValley',
            'stop_reason_code as stopReasonCode',
        ];

       
        //origin_payment作payment别名，指原价；payment作payment_real别名，指折后价

        if ($selectArr) {
            if (isset($selectArr['sumPayment']) && $selectArr['sumPayment'] == 1) {
                array_push($select, DB::raw('sum(abb_charge_order.payment_real) as sumPayment'));
            }
            if (isset($selectArr['sumElecCostFinal']) && $selectArr['sumElecCostFinal'] == 1) {
                array_push($select, DB::raw('sum(abb_charge_order.elec_cost_final) as sumElecCostFinal'));
            }
            if (isset($selectArr['sumServeCostFinal']) && $selectArr['sumServeCostFinal'] == 1) {
                array_push($select, DB::raw('sum(abb_charge_order.serve_cost_final) as sumServeCostFinal'));
            }
            if (isset($selectArr['sumElecQuantity']) && $selectArr['sumElecQuantity'] == 1) {
                array_push($select, DB::raw('sum(abb_charge_order.elec_quantity) as sumElecQuantity'));
            }
            if (isset($selectArr['sumDuration']) && $selectArr['sumDuration'] == 1) {
                array_push($select, DB::raw('sum(abb_charge_order.duration) as sumDuration'));
            }
        }

        

        $orderInfos = DB::table('charge_order')
                ->select($select)
                ->where('charge_order.is_del', 0);

        if ($ids) {
            $orderInfos = $orderInfos->whereIn('charge_order.id', $ids);
        }

        if (isset($data['id'])) {
            $orderInfos = $orderInfos->where('charge_order.id', $id);
        }

        if ($portId) {
            $orderInfos = $orderInfos->where('charge_order.port_id', $portId);
        }

        if (isset($data['portIds'])) {
            $orderInfos = $orderInfos->whereIn('charge_order.port_id', $portIds);
        }

        if (!empty($orQuery)) {
            $orderInfos = $orderInfos->where(function ($orderInfos) use ($orQuery) {
                if ($orQuery['order_number']) {
                    $orderInfos = $orderInfos->orWhere('charge_order.order_number', 'like', '%' . $orQuery['order_number'] . '%');
                }
                if ($orQuery['user_id']) {
                    $orderInfos = $orderInfos->orWhereIn('charge_order.user_id', $orQuery['user_id']);
                }
                if ($orQuery['or_user_id']) {
                    $orderInfos = $orderInfos->orWhereIn('charge_order.user_id', $orQuery['or_user_id']);
                }
                if ($orQuery['station_id']) {
                    $orderInfos = $orderInfos->orWhereIn('charge_order.station_id', $orQuery['station_id']);
                }
                if ($orQuery['device_id']) {
                    $orderInfos = $orderInfos->orWhereIn('charge_order.device_id', $orQuery['device_id']);
                }
            });
        }

        if (isset($data['stationIds'])) {
            $orderInfos = $orderInfos->whereIn('charge_order.station_id', $stationIds);
        }

        if (!empty($orderNumber)) {
            $orderInfos = $orderInfos->where('charge_order.order_number', 'like', '%' . $orderNumber . '%');
        }

        if (!empty($serialNumber)) {
            $orderInfos = $orderInfos->where('charge_order.serial_number', 'like', '%' . $serialNumber . '%');
        }

        if (isset($data['queryOrderNumber'])) {
            $orderInfos = $orderInfos->where('charge_order.order_number', $queryOrderNumber);
        }

        if ($stationId) {
            $orderInfos = $orderInfos->where('charge_order.station_id', $stationId);
        }
        
        if (isset($data['deviceIds'])) {
            $orderInfos = $orderInfos->whereIn('charge_order.device_id', $deviceIds);
        }

        if ($deviceId) {
            $orderInfos = $orderInfos->where('charge_order.device_id', $deviceId);
        }

        if ($userId) {
            $orderInfos = $orderInfos->where('charge_order.user_id', $userId);
        }

        if (isset($data['userIds'])) {
            $orderInfos = $orderInfos->whereIn('charge_order.user_id', $userIds);
        }

        if ($startSTime) {
            $orderInfos = $orderInfos->where('charge_order.started_at', '>=', $startSTime);
        }

        if ($endSTime) {
            $orderInfos = $orderInfos->where('charge_order.started_at', '<=', $endSTime);
        }

        if ($endSt) {
            $orderInfos = $orderInfos->where('charge_order.started_at', '<', $endSt);
        }

        if ($status) {//订单状态
            $orderInfos = $orderInfos->where('charge_order.status', $status);
        }

        if (isset($data['statusArr'])) {
            $orderInfos = $orderInfos->whereIn('charge_order.status', $statusArr);
        }

        if (isset($data['noStatusArr'])) {
            $orderInfos = $orderInfos->whereNotIn('charge_order.status', $noStatusArr);
        }
        
        if (isset($data['noOrderTypeArr'])) {
            $orderInfos = $orderInfos->whereNotIn('charge_order.order_type', $noOrderTypeArr);
        }
        
        if (isset($data['minDuration'])){
            $orderInfos = $orderInfos->where('charge_order.duration', '>=', $minDuration);
        }

        if (isset($data['minElecQuantity'])) {
            $orderInfos = $orderInfos->where('charge_order.elec_quantity', '>=', $minElecQuantity);
        }

        if (isset($data['minPayment'])) {
            $orderInfos = $orderInfos->where('charge_order.payment', '>=', $minPayment);
        }

        if (isset($data['minElecCostFinal'])) {
            $orderInfos = $orderInfos->where('charge_order.elec_cost_final', '>=', $minElecCostFinal);
        }

        if (isset($data['minServeCostFinal'])) {
            $orderInfos = $orderInfos->where('charge_order.serve_cost_final', '>=', $minServeCostFinal);
        }

        if ($isEndTime > 0) {
            if ($isEndTime == 1) {//有结束时间
                $orderInfos = $orderInfos->where('charge_order.finished_at', '<>', '')->where('charge_order.finished_at', '<>', null);
            }
        }
        $orderCount = $orderInfos->count();
        $result['total'] = $orderCount;
        $orderInfos->leftJoin('charge_order_segment', function($cosJoin) {
            $cosJoin->on('charge_order_segment.order_id', '=', 'charge_order.id')
                 ->where('charge_order_segment.is_del', 0);
        });

        if ($offset >= 0 && $limit > 0) {
            $orderInfos = $orderInfos->skip($offset)->take($limit);
        }

        $result['list'] = $orderInfos->orderBy('charge_order.'.$orderBy, $order)->get();
        return $result;
    }
    

    public function getChargeOrderNew($data) {
        $service = new OrderService();
        $orderData = $this->queryChargeOrder($data);
        $orderData['currency_dest'] = isset($data['currencys']['currency_dest']) ? $data['currencys']['currency_dest'] : 1;//货币费率
        $orderData['order'] = 'desc';
        $orderData['orderBy'] = 'id';
        if($data['downexcel'] == 1) {
            $query = [
                'type' => 'orderSegment',
                'filetitle' => 'order_',
                'sqlCount' => $service->dataCount($orderData),
                'pageSize' => 2000,
                'handler' => 111,
                'builder' => $service,
                'builderParam' => $orderData,
            ];
            $excelExport = new ExcelController($query);
            return $excelExport->execute();
        } else {
            return [
                'total' => $service->dataCount($orderData),
                'list' => $service->dataPageList($orderData),
            ];
        }
    }

    public function getOccupyOrder($data) {
        $service = new OccupyOrderService();
        $orderData = $this->queryChargeOrder($data);
        $orderData['currency_dest'] = isset($data['currencys']['currency_dest']) ? $data['currencys']['currency_dest'] : 1;//货币费率
        $orderData['order'] = 'desc';
        $orderData['orderBy'] = 'id';

        if($data['downexcel'] == 1) {
            $query = [
                'type' => 'occupyOrder',
                'filetitle' => 'order_',
                'sqlCount' => $service->dataCount($orderData),
                'pageSize' => 2000,
                'handler' => 111,
                'builder' => $service,
                'builderParam' => $orderData,
            ];
            $excelExport = new ExcelController($query);
            return $excelExport->execute();
        } else {
            return [
                'total' => $service->dataCount($orderData),
                'list' => $service->dataPageList($orderData),
            ];
        }
    }

    /**
     * 获取订单信息
     * @param $data
     * @return array
     */
    public function getChargeOrder($data)
    {
        //订单支付状态
        $langStatus = AccountHelper::transLang('charge_order_status');
        $status = is_array($langStatus) ? $langStatus : [];
        //订单启动方式
        $langType = AccountHelper::transLang('charge_order_start_type');
        $startType = is_array($langType) ? $langType : [];
        //订单充电状态
        $langOrderStatus = AccountHelper::transLang('charge_order_order_status');
        $orderStatus = is_array($langOrderStatus) ? $langOrderStatus : [];
        //订单开票大状态
        $langInvoiceStatus = AccountHelper::transLang('charge_order_show_invoice_status');
        $invoiceStatus = is_array($langInvoiceStatus) ? $langInvoiceStatus : [];
        //设备大状态的分布
        $invoiceStatusArray = TransHelper::ORDER_INVOICE_STATUS_ARRAY;
        $currencyDest = isset($data['currencys']['currency_dest']) ? $data['currencys']['currency_dest'] : 1;//货币费率
        
        $orderData = $this->queryChargeOrder($data);
        $result = $this->getJoinChargeOrderDB($orderData);
        
        $station = new Station();
        $device = new Device();
        $port = new DevicePort();        

        //用户信息
        $resUserIds = $result['list']->pluck('user_id')->unique()->all();
        $userData = [
            'ids' => $resUserIds,
            'offset' => 0,
            'limit' => 0
        ];
        $user = new User();
        $userDB = $user->getUserDB($userData);
        $users = $userDB['list']->keyBy('id')->all();
        $userIds = $userDB['list']->pluck('id')->unique()->all();
        
        //用户组信息
        $uguData = [
            'userIds' => $userIds,
            'offset' => 0,
            'limit' => 0
        ];
        $userGroupUser = new UserGroupUser();
        $uguDB = $userGroupUser->getUserGroupUserDB($uguData);
        $userGroupIds = $uguDB['list']->pluck('group_id')->unique()->all();
        
        $ugData = [
            'ids' => $userGroupIds,
            'offset' => 0,
            'limit' => 0
        ];
        $userGroup = new UserGroup();
        $userGroupDB = $userGroup->getUserGroupDB($ugData);
        $userGroupDBRes = $userGroupDB['list']->keyBy('id')->all();
        
        $groups = [];
        foreach ($uguDB['list'] as $uVal) {
            if ($uVal['group_id'] <= 0) {
                continue;
            } 
            $name = isset($userGroupDBRes[$uVal['group_id']]['name']) ? $userGroupDBRes[$uVal['group_id']]['name'] : '';
            $groups[$uVal['user_id']][] = [
                'id' => $uVal['group_id'], 
                'name' => $name
            ];
        }
        
        //设备信息
        $resDeviceIds = $result['list']->pluck('device_id')->unique()->all();
        $deviceData = [
            'deviceIds' => $resDeviceIds,
            'offset' => 0,
            'limit' => 0
        ];
        $deviceDB = $device->getDeviceDB($deviceData);
        $devices = $deviceDB['list']->keyBy('id')->all();

        //枪信息
        $resPortIds = $result['list']->pluck('port_id')->unique()->all();
        $portData = [
            'ids' => $resPortIds,
            'offset' => 0,
            'limit' => 0
        ];
        $portDB = $port->getPortDB($portData);
        $ports = $portDB['list']->keyBy('id')->all();

        //站点信息
        $resStationIds = $result['list']->pluck('station_id')->unique()->all();
        $staData = [
            'ids' => $resStationIds,
            'offset' => 0,
            'limit' => 0
        ];
        $stationDB = $station->getStationDB($staData);
        $stations = $stationDB['list']->keyBy('id')->all();
        
        //站点分时计费信息
//        $resOrderIds = $result['list']->pluck('id')->unique()->all();
//        $chargeOrderSegment = new ChargeOrderSegment();
//        $segmentData = [
//            'orderIds' => $resOrderIds,
//            'offset' => 0,
//            'limit' => 0
//        ];
//        $segmentDB = $chargeOrderSegment->getChargeOrderSegmentDB($segmentData);
//        $orderSegments = $segmentDB['list']->keyBy('order_id')->all();

        //公司信息
        $entIds = $stationDB['list']->pluck('enterprise_id')->all();
        $enterprise = new Enterprise();
        $enterpriseData = [
            'ids' => $entIds,
            'offset' => 0,
            'limit' => 0
        ];
        $enterpriseDB = $enterprise->getEnterpriseDB($enterpriseData);
        $enterprises = $enterpriseDB['list']->keyBy('id')->all();
        
        //获取订单的开票状态，对应的大状态
        $invoices = [];
        foreach ($invoiceStatusArray as $isaKey => $isaVal) {
            foreach ($isaVal as $iVal) {
                $invoices[$iVal]['key'] = isset($invoiceStatus[$isaKey]) ? $isaKey : 0;
                $invoices[$iVal]['name'] = isset($invoiceStatus[$isaKey]) ? $invoiceStatus[$isaKey] : '';
            }
        }

        if(memory_get_usage() > (1024 * 1024 * 260)) {
            throw new FatalErrorException('memory_limit',5005,'','','');
        }
        $chargeStopDetail = TransHelper::CHARGE_STOP_REASON_DETAIL;
        $result['list'] = $result['list']->map(function ($item) use ($users, $devices, $stations, $status, $currencyDest, $ports, $enterprises, $orderStatus, $invoices, $groups, $startType, $chargeStopDetail) {
            $module = $item;

            //订单信息
            //订单号
            $module['orderNumber'] = $module['order_number'];
            unset($module['order_number']);
            //本地开始时间（精确到秒）
            $module['startedAt'] = AccountHelper::transTime($module['started_at'], 1);
            unset($module['started_at']);
            //本地结束时间（精确到秒）
            $module['finishedAt'] = AccountHelper::transTime($module['finished_at'], 1);
            unset($module['finished_at']);
            //电量（kw/h）
            $module['elecQuantity'] = $module['elec_quantity'] > 0 ? round($module['elec_quantity'] / 1000, 3) : 0;
            //原价总花费（元）
            $module['originPayment'] = AccountHelper::transPayment($module['origin_payment'], $currencyDest);
            //折后总花费（元）
            $module['payment'] = AccountHelper::transPayment($module['payment'], $currencyDest);
            //时长（s）
            $module['duration'] = $module['duration'] > 0 ? $module['duration'] : 0;
           
            //原价电费（元）
            $module['elecCost'] = AccountHelper::transPayment($module['elec_cost'], $currencyDest);
            //原价服务费（元）
            $module['serveCost'] = AccountHelper::transPayment($module['serve_cost'], $currencyDest);
            //折后电费（元）
            $module['elecCostFinal'] = AccountHelper::transPayment($module['elec_cost_final'], $currencyDest);
            //折后服务费（元）
            $module['serveCostFinal'] = AccountHelper::transPayment($module['serve_cost_final'], $currencyDest);

            //订单支付状态名
            $module['statusName'] = isset($status[$module['status']]) ? $status[$module['status']] : $module['status'];
            
            //订单充电状态名
            $module['orderStatusName'] = isset($orderStatus[$module['orderStatus']]) ? $orderStatus[$module['orderStatus']] : $module['orderStatus'];

            //订单开票大状态
            $module['invoice'] = isset($invoices[$module['invoiceStatus']]['key']) ? $invoices[$module['invoiceStatus']]['key'] : 0;
            //订单开票大状态名
            $module['invoiceName'] = isset($invoices[$module['invoiceStatus']]['name']) ? $invoices[$module['invoiceStatus']]['name'] : '';
            //启动方式
            $module['startType'] = isset($startType[$module['start_type']]) ? $startType[$module['start_type']] : $module['start_type'];
            
            //起止SOC
            $module['startSoc'] = ($module['startSoc'] >= 0) ? $module['startSoc'] : '';
            $module['endSoc'] = ($module['endSoc'] >= 0 && !empty($module['finishedAt'])) ? $module['endSoc'] : '';//无结束时间，结束soc为空

            //用户信息
            $module['userName'] = isset($users[$item['user_id']]['name']) ? $users[$item['user_id']]['name'] : '';
            $module['phone'] = isset($users[$item['user_id']]['bind_mail']) ? $users[$item['user_id']]['bind_mail'] : '';//绑定手机号
            $module['authen'] = isset($users[$item['user_id']]['authen']) ? $users[$item['user_id']]['authen'] : '';//用户编号
            
            //用户组信息
            $module['userGroups'] = isset($groups[$item['user_id']]) ? $groups[$item['user_id']] : [];

            //站点信息
            $module['stationName'] = isset($stations[$item['station_id']]['name']) ? $stations[$item['station_id']]['name'] : '';

            //设备别名，桩号
            $module['aliasNumber'] = isset($devices[$item['device_id']]['alias_number']) ? $devices[$item['device_id']]['alias_number'] : '';
            $module['deviceNumber'] = isset($devices[$item['device_id']]['device_number']) ? $devices[$item['device_id']]['device_number'] : '';
            $module['modelName'] = isset($devices[$item['device_id']]['model']) ? $devices[$item['device_id']]['model'] : '';

            //端口号
            $module['portNumber'] = isset($ports[$item['port_id']]['port_number']) ? AccountHelper::transPortNumber($ports[$item['port_id']]['port_number']) : '';

            //公司信息
            $module['enterpriseId'] = isset($enterprises[$stations[$item['station_id']]['enterprise_id']]) ? $enterprises[$stations[$item['station_id']]['enterprise_id']]['id'] : 0;
            $module['enterpriseName'] = isset($enterprises[$stations[$item['station_id']]['enterprise_id']]) ? $enterprises[$stations[$item['station_id']]['enterprise_id']]['name'] : '';
            $module['stopReasonCode'] = ($item['stopReasonCode']==-1)?'':$item['stopReasonCode'];
            
            //充电停止详情
            $module['finishReasonDetail'] = isset($chargeStopDetail[$item['finishReasonDetail']])?$chargeStopDetail[$item['finishReasonDetail']]:'未知';
            //订单按照尖峰平谷计费
            // if ($module['fee_type'] == 2) {
                $elecCostSharp = $module['elecCostSharp'] ? $module['elecCostSharp'] : 0;
                $module['elecCostSharp'] = AccountHelper::transPayment($elecCostSharp, $currencyDest);//尖时原价电费（元）
                $elecCostPeak = $module['elecCostPeak'] ? $module['elecCostPeak'] : 0;
                $module['elecCostPeak'] = AccountHelper::transPayment($elecCostPeak, $currencyDest);//峰时原价电费（元）
                $elecCostAverage = $module['elecCostAverage'] ? $module['elecCostAverage'] : 0;
                $module['elecCostAverage'] = AccountHelper::transPayment($elecCostAverage, $currencyDest);//平时原价电费（元）
                $elecCostValley = $module['elecCostValley'] ? $module['elecCostValley'] : 0;
                $module['elecCostValley'] = AccountHelper::transPayment($elecCostValley, $currencyDest);//谷时原价电费（元）
                
                $quantitySharp = $module['quantitySharp'] ? $module['quantitySharp'] : 0;
                $module['quantitySharp'] = $quantitySharp > 0 ? round($quantitySharp / 1000, 3) : 0;//尖时电量（kw/h）
                $quantityPeak = $module['quantityPeak'] ? $module['quantityPeak'] : 0;
                $module['quantityPeak'] = $quantityPeak > 0 ? round($quantityPeak / 1000, 3) : 0;//峰时电量（kw/h）
                $quantityAverage = $module['quantityAverage'] ? $module['quantityAverage'] : 0;
                $module['quantityAverage'] = $quantityAverage > 0 ? round($quantityAverage / 1000, 3) : 0;//平时电量（kw/h）
                $quantityValley = $module['quantityValley'] ? $module['quantityValley'] : 0;
                $module['quantityValley'] = $quantityValley > 0 ? round($quantityValley / 1000, 3) : 0;//谷时电量（kw/h）
            // } else {
            //     $module['elecCostSharp'] = 0;//尖时原价电费（元）
            //     $module['elecCostPeak'] = 0;//峰时原价电费（元）
            //     $module['elecCostAverage'] = 0;//平时原价电费（元）
            //     $module['elecCostValley'] = 0;//谷时原价电费（元）
            //     $module['quantitySharp'] = 0;//尖时电量（kw/h）
            //     $module['quantityPeak'] = 0;//峰时电量（kw/h）
            //     $module['quantityAverage'] = 0;//平时电量（kw/h）
            //     $module['quantityValley'] = 0;//谷时电量（kw/h）
            // }
            
            return $module;
        })->all();

        //下载excel
        $downexcel = isset($data['downexcel']) ? $data['downexcel'] : 0;
        $orders_count = $result['total'] > 0 ? count($result['list']) : 0;
        if ($downexcel == 1) {
            $query = [
                'type' => 'orderSegment',
                'filetitle' => 'order_',
                'sqlCount' => $orders_count,
                'pageSize' => 2000,
                'handler' => $result['list'],
                'isArray' => 1
            ];
            $excelExport = new ExcelController($query);
            return $excelExport->execute();
        }

        return $result;
    }


    public function getOccupyOrderList($data) {
        $offset = isset($data['offset']) ? $data['offset'] : 0;
        $limit = isset($data['limit']) ? $data['limit'] : 20;
        $orderBy = isset($data['orderBy']) ? $data['orderBy'] : 'id';
        $order = isset($data['order']) ? $data['order'] : 'desc';
        $downexcel = isset($data['downexcel']) ? $data['downexcel'] : 0;

        $data = $this->queryChargeOrder($data);
        //订单支付状态
        $langStatus = AccountHelper::transLang('charge_order_status');
        $orderStatus = is_array($langStatus) ? $langStatus : [];
        $currencyDest = isset($data['currencys']['currency_dest']) ? $data['currencys']['currency_dest'] : 1;//货币费率



        $status = isset($data['status']) ? $data['status'] : 0;
        $orderNumber = isset($data['orderNumber']) ? $data['orderNumber'] : '';
        $stationId = isset($data['stationId']) ? $data['stationId'] : 0;
        $enterpriseId = isset($data['enterpriseId']) ? $data['enterpriseId'] : 0;
        $deviceId = isset($data['deviceId']) ? $data['deviceId'] : 0;
        $userId = isset($data['userId']) ? $data['userId'] : 0;
        $portNumber = isset($data['portNumber']) ? $data['portNumber'] : 0;
        $startSTime = isset($data['startSTime']) ? $data['startSTime'] : '';
        $endSTime = isset($data['endSTime']) ? $data['endSTime'] : '';

        $query = DB::table('occupy_order')->where('is_del', 0);

        if($orderNumber) {
            $query->where('order_number', $orderNumber);
        }
        if($stationId) {
            $query->where('station_id', $stationId);
        }        
        if($deviceId) {
            $query->where('device_id', $deviceId);
        }        
        if($userId) {
            $query->where('user_id', $userId);
        }        
        if($startSTime) {
            $query->where('occupy_started_at', $orderNumber);
        }        
        if($endSTime) {
            $query->where('occupy_stoped_at', $orderNumber);
        }    
        if($enterpriseId) {
            $query->where('enterprise_id', $enterpriseId);
        }      
        if($portNumber) {
            $query->where('port_number', $portNumber);
        }        


        $result['total'] = $query->count();

        if ($offset >= 0 && $limit > 0) {
            $query->skip($offset)->take($limit);
        }       
        $select = [
            'id',
            'order_number',
            'station_id',
            'device_id',
            'device_number',
            'port_id',
            'port_number',
            'order_id',
            'user_id',
            'card_id',
            'duration',
            'status',
            'occupy_started_at as started_at',
            'occupy_stoped_at as finished_at',
            'price as origin_payment',
            'price as payment',
            'create_at'
        ];
        $list = $query->orderBy($orderBy, $order)->select($select)->get();
        $stationIds = $list->pluck('station_id')->unique()->all();
        $stations = DB::table('station')->where('is_del', 0)->whereIn('id', $stationIds)->select(['id', 'name'])->get();
        $stations = $stations->keyBy('id')->all();

        $result['list'] = $list->map(function($item) use ($currencyDest, $stations, $orderStatus) {
            $module['id'] = $item['id'];
            //订单号
            $module['orderNumber'] = $item['order_number'];
            $module['deviceNumber'] = $item['device_number'];
            $module['portNumber'] = $item['port_number'];
            //本地开始时间（精确到秒）
            $module['startedAt'] = AccountHelper::transTime($item['started_at'], 1);
            //本地结束时间（精确到秒）
            $module['finishedAt'] = AccountHelper::transTime($item['finished_at'], 1);
            //折后总花费（元）
            $module['payment'] = AccountHelper::transPayment($item['payment'], $currencyDest);
            //时长（s）
            $module['duration'] =  $item['duration'];

            //订单支付状态名
            $module['statusName'] = isset($orderStatus[$item['status']]) ? $orderStatus[$item['status']] : $item['status'];
            
            //订单开票大状态
            $module['invoice'] = 0;
            //订单开票大状态名
            $module['invoiceName'] = '未开票';
            //站点信息
            $module['stationName'] = isset($stations[$item['station_id']]['name']) ? $stations[$item['station_id']]['name'] : '';

            //设备别名，桩号
            $module['deviceNumber'] = $item['device_number'];
            //端口号
            $module['startedAt'] = $item['started_at'];
            $module['finishedAt'] = $item['finished_at'];
            $module['createAt'] = $item['create_at'];
            return $module;
        });

        //下载excel
        if ($downexcel == 1) {
            $query = [
                'type' => 'occupyOrder',
                'filetitle' => 'order_',
                'sqlCount' => $result['total'],
                'pageSize' => 2000,
                'handler' => $result['list']->all(),
                'isArray' => 1
            ];
            $excelExport = new ExcelController($query);
            return $excelExport->execute();
        }
        return $result;
    }


    /**
     * 通过id获取订单信息 DB
     * @param $id
     * @return array
     */
    public function getByOrderIdDB($id)
    {
        $result = DB::table('charge_order')
            ->select('id', 'order_number', 'station_id', 'device_id', 'port_id', 'status', 'order_status', 'started_at', 'finished_at')
            ->where('id', $id)
            ->where('is_del', 0)
            ->first();
        return $result;
    }

    /**
     * 获取订单详情
     * @param $data
     * @return array
     */
    public function getOrderDetails($data)
    {
        //订单状态
        $langStatus = AccountHelper::transLang('charge_order_status');
        $status = is_array($langStatus) ? $langStatus : [];
        $langType = AccountHelper::transLang('charge_order_start_type');
        $startType = is_array($langType) ? $langType : [];
        $langChargeType = AccountHelper::transLang('charge_order_charge_type');
        //订单停止原因类型
        $langReason = AccountHelper::transLang('charge_order_charge_finish_reason');
        $finishReasons = is_array($langReason) ? $langReason : [];
        //订单远程停止原因类型
        $langReasonDetail = AccountHelper::transLang('charge_order_finish_reason_detail');
        $finishReasonDetails = is_array($langReasonDetail) ? $langReasonDetail : [];
        $chargeType = is_array($langChargeType) ? $langChargeType : [];
        $currencyDest = isset($data['currencys']['currency_dest']) ? $data['currencys']['currency_dest'] : 1;//货币费率

        //订单信息
        $orderData = [
            'id' => isset($data['id']) ? $data['id'] : 0,
            'offset' => 0,
            'limit' => 0
        ];
        $result = $this->getChargeOrderDB($orderData);


        $resUserIds = $result['list']->pluck('user_id')->unique()->all();
        $userData = [
            'ids' => $resUserIds,
            'offset' => 0,
            'limit' => 0
        ];
        $user = new User();
        $userDB = $user->getUserDB($userData);
        $users = $userDB['list']->keyBy('id')->all();

        $resDeviceIds = $result['list']->pluck('device_id')->unique()->all();
        $deviceData = [
            'deviceIds' => $resDeviceIds,
            'offset' => 0,
            'limit' => 0
        ];
        $device = new Device();
        $deviceDB = $device->getDeviceDB($deviceData);
        $devices = $deviceDB['list']->keyBy('id')->all();

        $resPortIds = $result['list']->pluck('port_id')->unique()->all();
        $portData = [
            'ids' => $resPortIds,
            'offset' => 0,
            'limit' => 0
        ];
        $port = new DevicePort();
        $portDB = $port->getPortDB($portData);
        $ports = $portDB['list']->keyBy('id')->all();

        //站点信息
        $stationIds = $result['list']->pluck('station_id')->unique()->all();
        $stationData = [
            'ids' => $stationIds,
            'offset' => 0,
            'limit' => 0
        ];
        $station = new Station();
        $stationDB = $station->getStationDB($stationData);
        $stations = $stationDB['list']->keyBy('id')->all();

        $entIds = $stationDB['list']->pluck('enterprise_id')->all();
        $enterprise = new Enterprise();
        $enterpriseData = [
            'ids' => $entIds,
            'offset' => 0,
            'limit' => 0
        ];
        $enterpriseDB = $enterprise->getEnterpriseDB($enterpriseData);
        $enterprises = $enterpriseDB['list']->keyBy('id')->all();

        $result['list'] = $result['list']->map(function ($item) use ($users, $devices, $ports, $status, $currencyDest, $stations, $enterprises, $startType, $chargeType, $finishReasons, $finishReasonDetails) {
            $module = $item;

            //订单信息
            //订单号
            $module['orderNumber'] = $module['order_number'];
            unset($module['order_number']);
            //电量（kw/h）
            $module['elecQuantity'] = $module['elec_quantity'] > 0 ? round($module['elec_quantity'] / 1000, 3) : 0;
            //折后总花费（元）
            $module['payment'] = AccountHelper::transPayment($module['payment'], $currencyDest);
            //折后电费（元）
            $module['elecCostFinal'] = AccountHelper::transPayment($module['elec_cost_final'], $currencyDest);
            //折后服务费（元）
            $module['serveCostFinal'] = AccountHelper::transPayment($module['serve_cost_final'], $currencyDest);
            //电费（元）
            $module['elecCost'] = AccountHelper::transPayment($module['elec_cost'], $currencyDest);
            //服务费（元）
            $module['serveCost'] = AccountHelper::transPayment($module['serve_cost'], $currencyDest);
            //订单是否异常（1-正常（包括1正常，3校正，4余额不足），2-异常）
            $module['orderType'] = $module['order_type'] == 2 ? 2 : 1;
            unset($module['order_type']);
            //订单结束原因
            $module['finishReasonName'] = isset($finishReasons[$module['chargeFinishReason']]) ? $finishReasons[$module['chargeFinishReason']] : '';
            //订单远程停止原因
            $module['finishReasonDetailName'] = isset($finishReasonDetails[$module['finishReasonDetail']]) ? $finishReasonDetails[$module['finishReasonDetail']] : '';

            //订单状态名称
            $module['statusName'] = isset($status[$module['status']]) ? $status[$module['status']] : $module['status'];
            //启动方式
            $module['startType'] = isset($startType[$module['start_type']]) ? $startType[$module['start_type']] : $module['start_type'];
            $module['chargeType'] = isset($chargeType[1]) ? $chargeType[1] : '立即开始';//默认1-立即开始，暂未区分 预约充电（TO DO）
            
            //用户信息
            $module['userId'] = isset($users[$item['user_id']]['authen']) ? $users[$item['user_id']]['authen'] : '';
            unset($module['user_id']);
            $module['userName'] = isset($users[$item['user_id']]['name']) ? $users[$item['user_id']]['name'] : '';
            $module['phone'] = isset($users[$item['user_id']]['bind_mail']) ? $users[$item['user_id']]['bind_mail'] : '';

            //设备信息
            $module['aliasNumber'] = isset($devices[$item['device_id']]['alias_number']) ? $devices[$item['device_id']]['alias_number'] : '';
            $module['deviceNumber'] = isset($devices[$item['device_id']]['device_number']) ? $devices[$item['device_id']]['device_number'] : '';
            $module['powerType'] = isset($devices[$item['device_id']]['power_type']) ? $devices[$item['device_id']]['power_type'] : '';
            $module['elecPower'] = isset($devices[$item['device_id']]['elec_power']) ? round($devices[$item['device_id']]['elec_power'] / 1000, 2) : '';//额定功率（kw）

            //该订单的开始时间，距该端口上一条订单的结束时间之差
            $orderData = [
                'portId' => $module['port_id'],
                'endSt' => $module['started_at'],
                'offset' => 0,
                'limit' => 1
            ];
            $lastRes = $this->getPortLastOrder($orderData);
            // $lastEndTime = ($lastRes['total'] > 0) ? $lastRes['list'][0]['finished_at'] : '';
            $lastEndTime = empty($lastRes) ? '' : $lastRes['finished_at'];
            $hour = round((strtotime($module['started_at']) - strtotime($lastEndTime)) / 3600);
            $minute = round((strtotime($module['started_at']) - strtotime($lastEndTime)) % 3600 / 60);
            $module['lastOrderHour'] = $hour;//时间差-小时
            $module['lastOrderMinute'] = $minute;//时间差-分钟
            if (!$lastEndTime) {//无结束时间
                $module['lastOrder'] = '';
            } else if ($hour > 720) {//30天
                $module['lastOrder'] = '1 month ago';
            } else if ($hour > 0) {
                $module['lastOrder'] = $minute > 0 ? $hour . 'h ' . $minute . 'mins ago' : $hour . ' h ago';
            } else if ($minute > 0) {
                $module['lastOrder'] = $minute . ' mins ago';
            } else {
                $module['lastOrder'] = '';
            }
            //本地开始时间
            $module['startedAt'] = AccountHelper::transTime($module['started_at']);
            //本地结束时间
            $module['finishedAt'] = AccountHelper::transTime($module['finished_at']);
            
            //起止SOC
            $module['startSoc'] = ($module['startSoc'] >= 0) ? $module['startSoc'] : '';
            $module['endSoc'] = ($module['endSoc'] >= 0 && !empty($module['finishedAt'])) ? $module['endSoc'] : '';//无结束时间，结束soc为空
            
            //端口信息
            $module['portType'] = isset($ports[$item['port_id']]['port_type']) ? $ports[$item['port_id']]['port_type'] : 0;

            //站点信息
            $module['stationName'] = isset($stations[$item['station_id']]['name']) ? $stations[$item['station_id']]['name'] : '';

            //公司信息
            $module['enterpriseId'] = isset($enterprises[$stations[$item['station_id']]['enterprise_id']]) ? $enterprises[$stations[$item['station_id']]['enterprise_id']]['id'] : 0;
            $module['enterpriseName'] = isset($enterprises[$stations[$item['station_id']]['enterprise_id']]) ? $enterprises[$stations[$item['station_id']]['enterprise_id']]['name'] : '';
            $carLincese = DB::table('card_license_order')->where('is_del', 0)->where('serial_number', $module['serialNumber'])->first(['license_number']);
            $isSetParkfee = DB::table('station_parking_rule')->where('is_del',0)->where('type','<>',0)->where('station_id',$item['station_id'])->first(['id']);
            $module['carLicense'] = empty($isSetParkfee['id'])?'':(empty($carLincese['license_number'])?'-':$carLincese['license_number']); 
            //vin信息
            $module['vinCode'] = isset($item['vinCode']) && !empty($item['vinCode']) ? $item['vinCode'] : '';

            //从流水获取订单订单费用占比
            list($module['balanceVirtual'], $module['balanceOffline'], $module['balanceWechat']) = self::getOrderPayChannelDetailByOrderNumber($item['order_number']);

            return $module;
        });

        return $result['list'];
    }

    protected function getPortLastOrder($param) {
        $query = Db::table('charge_order')->where('is_del', 0);
        $portId = isset($param['portId'])?$param['portId']:0;
        $endSt = isset($param['endSt'])?$param['endSt']:'';

        if($portId) {
            $query->where('port_id', $portId);
        }
        if($endSt) {
            $query->where('started_at', '>', $endSt);
        }
        $id = $query->first(['id']);
        $res = [];
        if($id) {
            $res = Db::table('charge_order')->where('id', $id)->first([
                'finished_at'
            ]);
        }
        return $res;
    }

    /**
     * 人工订单处理
     * @param $data
     * @return array
     */
    public function process($data)
    {
        $result = [
            'msg' => ''
        ];

        $device = new Device();
        $deviceInfo = $device->getByDeviceIdDB($data['deviceId']);
        $deviceNumber = isset($deviceInfo['device_number']) ? $deviceInfo['device_number'] : '';

        $devicePort = new DevicePort();
        $portInfo = $devicePort->getByPortIdDB($data['portId']);
        $portNumber = isset($portInfo['port_number']) ? $portInfo['port_number'] : '';

        $typeArr = [
            1 => 'AUTO_DEDUCTION',
            2 => 'ERROR_STATE',
            3 => 'MANUAL_REFUND'
        ];
        $processingType = isset($typeArr[$data['type']]) ? $typeArr[$data['type']] : '';

        $entity = [
            'orderNumber' => $data['orderNumber'],
            'enterpriseId' => $data['enterpriseId'],
            'stationId' => $data['stationId'],
            'deviceNumber' => $deviceNumber,
            'portNumber' => $portNumber,
            'processingType' => $processingType,
        ];

        //调用请求清除
        $msg = $this->reqProcess($entity);
        $result['msg'] = $msg;
        $result['code'] = empty($msg)?0:1;

        return $result;
    }

    /**
     * 人工订单处理
     * @param $data
     * @return string
     */
    private function reqProcess($data)
    {
        $msg = '';
        $url = env('ABB_API_CHAGING', 'http://10.0.1.4') . ':18765/chargeOrder/order/v1/unusual_order_manage';
        $info = [
            'orderNumber' => isset($data['orderNumber']) ? (string)$data['orderNumber'] : '',//订单号
            'enterpriseId' => isset($data['enterpriseId']) ? (int)$data['enterpriseId'] : 0,//公司id
            'stationId' => isset($data['stationId']) ? (int)$data['stationId'] : 0,//站点id
            'deviceNumber' => isset($data['deviceNumber']) ? (string)$data['deviceNumber'] : '',//设备号
            'portNumber' => isset($data['portNumber']) ? (string)$data['portNumber'] : '',//端口号
            'processingType' => isset($data['processingType']) ? (string)$data['processingType'] : 0,//处理类型
        ];
        if (empty($info['orderNumber']) || empty($info['deviceNumber']) || empty($info['portNumber']) || empty($info['enterpriseId']) || empty($info['stationId']) || empty($info['processingType'])) {
            return $msg = 'content_format';
        }
        $res = ChargeHelper::reqCharge($url, $info, 'ChargeOrder', 'reqProcess');
        if ($res['code'] == 1) {
            return $msg = $res['msg'];
        } else {
            if (isset($res['body']['Ret']) && $res['body']['Ret'] == 1) {
                return $msg = $res['body']['Msg'];
            }
        }
        return $msg;
    }
    

    //统计的模糊查询筛选项
    public function queryStatistics($data) {
        $enterpriseIds = $data['enterpriseIds'];

        $query = isset($data['query'])?$data['query']:'';     
        if(empty($query)){
            return [];
        }
        //模糊搜索用户手机号匹配用户id
        $userData = [
            'bindMail' => $query,
            'offset' => 0,
            'limit' => 0
        ];
        $user = new User();
        $userDB = $user->getUserDB($userData);
        $userIds = $userDB['list']->pluck('id')->all();//用户手机号搜索的用户ids

        //模糊搜索用户组匹配用户id
        $userGroupData = [
            'enterpriseIds' => $enterpriseIds,
            'query' => $query,
            'offset' => 0,
            'limit' => 0
        ];
        $userGroup = new UserGroup();
        $userGroupDB = $userGroup->getUserGroupDB($userGroupData);
        $userGroupIds = $userGroupDB['list']->pluck('id')->all();

        if (!empty($userGroupIds)) {
            $uguData = [
                'groupIds' => $userGroupIds,
                'offset' => 0,
                'limit' => 0
            ];
            $userGroupUser = new UserGroupUser();
            $uguDB = $userGroupUser->getUserGroupUserDB($uguData);
            $ugUserIds = $uguDB['list']->pluck('user_id')->unique()->all();//用户组搜索的用户ids
        } else {
            $ugUserIds = [];
        }


        //模糊搜索站点组匹配站点id
        $staGroupData = [
            'enterpriseIds' => $enterpriseIds,
            'query' => $query,
            'offset' => 0,
            'limit' => 0
        ];
        $stationGroup = new StationGroup();
        $staGroupDB = $stationGroup->getStationGroupDB($staGroupData);
        $staGroupIds = $staGroupDB['list']->pluck('id')->all();

        $station = new Station();
        if (!empty($staGroupIds)) {
            $stationData = [
                'groupIds' => $staGroupIds,
                'offset' => 0,
                'limit' => 0
            ];
            $siteDB = $station->getStationDB($stationData);
            $siteIds = $siteDB['list']->pluck('id')->all();//站点组搜索的站点ids
        } else {
            $siteIds = [];
        }
        
        //模糊搜索站点匹配站点id
        $siteData = [
            'query' => $query,
            'offset' => 0,
            'limit' => 0
        ];
        $siteQueryDB = $station->getStationDB($siteData);
        $siteQueryIds = $siteQueryDB['list']->pluck('id')->all();//站点搜索的站点ids
        $siteIds = array_unique(array_merge($siteIds, $siteQueryIds));//取去重的并集
    
        $query = isset($data['query']) ? $data['query'] : '';
        return [
            'order_number' => $query,
            'user_id' => $userIds,
            'or_user_id' => $ugUserIds,
            'station_id' => $siteIds,
        ];//多字段模糊匹配项

    }

    /**
     * 处理订单的查询条件（订单列表、订单统计看板）
     */
    public function queryChargeOrder($data)
    {
        if (isset($data['enterpriseId']) && $data['enterpriseId'] > 0) {
            $enterpriseIds = array_intersect($data['enterpriseIds'], [$data['enterpriseId']]);
        } else {
            $enterpriseIds = $data['enterpriseIds'];
        }
        $stationData = [
            'offset' => 0,
            'limit' => 0,
            'enterpriseIds' => $enterpriseIds,
//            'id' => isset($data['id']) ? $data['id'] : 0
        ];
        $station = new Station();
        $resultId = $station->getStationDB($stationData);
        $stationIds = $resultId['list']->pluck('id')->all();

        $device = new Device();
        $port = new DevicePort();
        $query = isset($data['query']) ? $data['query'] : '';
        if (!empty($query)) {//简洁搜索

            //模糊搜索用户手机号匹配用户id
            $userData = [
                'bindMail' => $query,
                'offset' => 0,
                'limit' => 0
            ];
            $user = new User();
            $userDB = $user->getUserDB($userData);
            $userIds = $userDB['list']->pluck('id')->all();//用户手机号搜索的用户ids

            //模糊搜索用户组匹配用户id
            $userGroupData = [
                'enterpriseIds' => $enterpriseIds,
                'query' => $query,
                'offset' => 0,
                'limit' => 0
            ];
            $userGroup = new UserGroup();
            $userGroupDB = $userGroup->getUserGroupDB($userGroupData);
            $userGroupIds = $userGroupDB['list']->pluck('id')->all();

            if (!empty($userGroupIds)) {
                $uguData = [
                    'groupIds' => $userGroupIds,
                    'offset' => 0,
                    'limit' => 0
                ];
                $userGroupUser = new UserGroupUser();
                $uguDB = $userGroupUser->getUserGroupUserDB($uguData);
                $ugUserIds = $uguDB['list']->pluck('user_id')->unique()->all();//用户组搜索的用户ids
            } else {
                $ugUserIds = [];
            }


            //模糊搜索站点组匹配站点id
            $staGroupData = [
                'enterpriseIds' => $enterpriseIds,
                'query' => $query,
                'offset' => 0,
                'limit' => 0
            ];
            $stationGroup = new StationGroup();
            $staGroupDB = $stationGroup->getStationGroupDB($staGroupData);
            $staGroupIds = $staGroupDB['list']->pluck('id')->all();

            if (!empty($staGroupIds)) {
                $stationData = [
                    'groupIds' => $staGroupIds,
                    'offset' => 0,
                    'limit' => 0
                ];
                $siteDB = $station->getStationDB($stationData);
                $siteIds = $siteDB['list']->pluck('id')->all();//站点组搜索的站点ids
            } else {
                $siteIds = [];
            }
            
            //模糊搜索站点匹配站点id
            $siteData = [
                'query' => $query,
                'offset' => 0,
                'limit' => 0
            ];
            $siteQueryDB = $station->getStationDB($siteData);
            $siteQueryIds = $siteQueryDB['list']->pluck('id')->all();//站点搜索的站点ids
            $siteIds = array_unique(array_merge($siteIds, $siteQueryIds));//取去重的并集
            
            //模糊搜索设备匹配设备id
            $chargeData = [
                'query' => $query,
                'offset' => 0,
                'limit' => 0
            ];
            $chargeDB = $device->getDeviceDB($chargeData);
            $deviceIds = $chargeDB['list']->pluck('id')->all();//设备搜索的设备ids
            

            $query = isset($data['query']) ? $data['query'] : '';
            $orQuery = [
                'order_number' => $query,
                'user_id' => $userIds,
                'or_user_id' => $ugUserIds,
                'station_id' => $siteIds,
                'device_id' => $deviceIds
            ];//多字段模糊匹配项

            $orderData = [
                'offset' => isset($data['offset']) ? $data['offset'] : 0,
                'limit' => isset($data['limit']) ? $data['limit'] : 20,
                'stationIds' => $stationIds,
                'orQuery' => $orQuery,
                'status' => isset($data['status']) ? $data['status'] : 0
            ];
            //多选订单状态
            if (isset($data['statusArr']) && !empty($data['statusArr'])) {
                $orderData['statusArr'] = $data['statusArr'];
            }
//            $result = $this->getChargeOrderDB($orderData);

        } else {

            //搜索枪口号
            $portIds = [];
            if (isset($data['portNumber']) && $data['portNumber']) {
                $portData = [
                    'portNumber' => $data['portNumber'],
                    'offset' => 0,
                    'limit' => 0
                ];
                $searchPortDB = $port->getPortDB($portData);
                $portIds = $searchPortDB['list']->pluck('id')->all();
                
            }
            
            //搜索用户组
            $userGroupId = isset($data['userGroupId']) ? $data['userGroupId'] : 0;
            if ($userGroupId > 0) {
                $uguData = [
                    'groupId' => $userGroupId,
                    'offset' => 0,
                    'limit' => 0
                ];
                $userGroupUser = new UserGroupUser();
                $uguDB = $userGroupUser->getUserGroupUserDB($uguData);
                $usuIds = $uguDB['list']->pluck('user_id')->unique()->all();//用户组搜索的用户ids
                $ugUserIds = empty($usuIds) ? [-1] : $usuIds;//若用户组下无用户，避免全搜索
            } else {
                $ugUserIds = [];
            }
            
            //搜索用户
            $phone = isset($data['phone']) ? $data['phone'] : '';
            if (!empty($phone)) {
                //模糊搜索用户手机号匹配用户id
                $userData = [
                    'bindMail' => $phone,
                    'offset' => 0,
                    'limit' => 0
                ];
                $user = new User();
                $userDB = $user->getUserDB($userData);
                $uIds = $userDB['list']->pluck('id')->all();//用户手机号搜索的用户ids
                $pUserIds = empty($uIds) ? [-1] : $uIds;//若模糊的手机号搜不到用户，避免全搜索
            } else {
                $pUserIds = [];
            }
            
            //处理用户id的集合
            if ($ugUserIds == [-1] || $pUserIds == [-1]) {
                $userIds = [-1];//未搜索到数据
            } else if ($ugUserIds == [] || $pUserIds == []) {
                $userIds = array_merge($ugUserIds, $pUserIds);//取并集
            } else {
                $intersectIds = array_intersect($ugUserIds, $pUserIds);//取交集
                $userIds = empty($intersectIds) ? [-1] : $intersectIds;
            }

            $orderData = [
                'offset' => isset($data['offset']) ? $data['offset'] : 0,
                'limit' => isset($data['limit']) ? $data['limit'] : 20,
                'stationIds' => $stationIds,
                'orderNumber' => isset($data['orderNumber']) ? $data['orderNumber'] : '',
                'status' => isset($data['status']) ? $data['status'] : 0,
                'stationId' => isset($data['stationId']) ? $data['stationId'] : 0,
                'deviceId' => isset($data['deviceId']) ? $data['deviceId'] : 0,
                'userId' => isset($data['userId']) ? $data['userId'] : 0,
                'startSTime' => isset($data['st']) ? $data['st'] : '',
                'endSTime' => isset($data['et']) ? $data['et'] : '',
                'serialNumber' => isset($data['serialNumber']) ? $data['serialNumber'] : '',
                'startType' => isset($data['startType']) ? $data['startType'] : [],
            ];
            if (!empty($portIds)) {
                $orderData['portIds'] = $portIds;
                $orderData['portNumber'] = $data['portNumber'];
            }
            if (!empty($userIds)) {
                $orderData['userIds'] = $userIds;
            }
            //多选订单状态
            if (isset($data['statusArr']) && !empty($data['statusArr'])) {
                $orderData['statusArr'] = $data['statusArr'];
            }
//            $result = $this->getChargeOrderDB($orderData);

        }
        
        return $orderData;
    }
    
    public function getStatisticsNew($data) {
        $currencyDest = isset($data['currencys']['currency_dest']) ? $data['currencys']['currency_dest'] : 1;//货币费率
        $enterPriseId = isset($data['enterpriseId'])?$data['enterpriseId']:0;   //当前用户公司id
        $userId = isset($data['userId'])?$data['userId']:0;   //当前用户公司id

        $orderTotalInfo =  Statistics::getChargeOrderCount($userId, $enterPriseId); //订单总数和用户数据
        $durationTotalInfo = Statistics::getChargeOrderDurationTotal($userId, $enterPriseId);
        $elecQuantityTotalInfo = Statistics::getChargeOrderElecQuantityTotal($userId, $enterPriseId);
        $dcElecQuantityTotalInfo = Statistics::getChargeOrderDcElecQuantityTotal($userId, $enterPriseId);
        $elecPayTotalInfo = Statistics::getChargeOrderElePayTotal($userId, $enterPriseId);
        $elecServiceTotalInfo = Statistics::getChargeOrderServicePayTotal($userId, $enterPriseId);

        $totalServePay = AccountHelper::transPayment($elecServiceTotalInfo['servePay'],$currencyDest, 0, 1);
        $totalElePay = AccountHelper::transPayment($elecPayTotalInfo['elePay'],$currencyDest, 0, 1);

        return [
            'totalCount' => $orderTotalInfo['orderCount'],
            'orderUserCnt' => $orderTotalInfo['orderUserCount'],
            'totalDuration' => $durationTotalInfo['durationTotal'],
            'totalElecQuantity' => (int)ceil($elecQuantityTotalInfo['ElecQuantityTotal']/1000),
            'dcElecQuanlity' => (int)ceil($dcElecQuantityTotalInfo/1000),
            'totalElecPay' => $totalElePay,
            'totalServePay' => $totalServePay,
            'totalPayment' => $totalElePay + $totalServePay,

        ];
    }

    public function getOrderStatistics($data) {
        $currencyDest = isset($data['currencys']['currency_dest']) ? $data['currencys']['currency_dest'] : 1;//货币费率
        $orderTotal = $this->orderStatics($data, ['orderNum'=>1]);
        $durationTotal = $this->orderStatics($data, ['sumDuration'=>1]);
        $payment = $this->orderStatics($data, ['sumPayment'=>1]);
        $elec = $this->orderStatics($data, ['sumElecQuantity'=>1]);

        return [
            'totalCount' => !empty($orderTotal)?$orderTotal:0,
            'totalDuration' => !empty($durationTotal['sumDuration'])?$durationTotal['sumDuration']:0,
            'totalElecQuantity' => !empty($elec['sumElecQuantity'])?(int)ceil($elec['sumElecQuantity']/1000):0,
            'totalPayment' => !empty($payment['sumPayment'])?AccountHelper::transPayment($payment['sumPayment'], $currencyDest, 0, 1):0,
        ];
    }

    /**
     * 获取订单信息
     * @param $data
     * @return array
     */
    public function getStatistics($data)
    {
        $currencyDest = isset($data['currencys']['currency_dest']) ? $data['currencys']['currency_dest'] : 1;//货币费率
        
        $orderData = $this->queryChargeOrder($data);
        
        //剔除异常和退款订单
        $orderData['noOrderTypeArr'] = [2];//非 异常
        $orderData['noStatusArr'] = [7, 8, 9];//非 7退款中8退款成功9退款失败
        
        //获取总统计数
        $orderData['offset'] = 0;
        $orderData['limit'] = 0;
        
        $chargeOrders = $this->getChargeOrderDB($orderData);
        $userIds = $chargeOrders['list']->pluck('user_id')->unique()->all();
        
        $totalCount = $chargeOrders['total'];//总订单数
        $orderUserCnt = count($userIds);//订单的用户数（去重）
        
        
        $orderData['minDuration'] = 0;
        $orderData['select'] = ['sumDuration' => 1];
        $orderDura = $this->getChargeOrderDB($orderData);
        $totalDura = $orderDura['total'] > 0 ? (int)$orderDura['list'][0]['sumDuration'] : 0;//总时长（s）
        unset($orderData['minDuration']);
        
        
        $orderData['minElecQuantity'] = 0;
        $orderData['select'] = ['sumElecQuantity' => 1];
        $orderElec = $this->getChargeOrderDB($orderData);
//        $totalElec = $orderElec['total'] > 0 ? round($orderElec['list'][0]['sumElecQuantity'] / 1000, 3) : 0;
        $totalElec = $orderElec['total'] > 0 ? (int)ceil($orderElec['list'][0]['sumElecQuantity'] / 1000) : 0;//总电量（kw/h）（进一取整）
        
        //获取直流设备集合
        $device = new Device();
        $deviceData = [
            'stationIds' => $orderData['stationIds'],
            'chargeType' => 1,//直流
            'offset' => 0,
            'limit' => 0
        ];
        $deviceDB = $device->getDeviceDB($deviceData);
        $deviceIds = $deviceDB['list']->pluck('id')->all();
        $orderData['deviceIds'] = $deviceIds;
        $dcTotalOrderElec = $this->getChargeOrderDB($orderData);
//        $dcTotalElec = $dcTotalOrderElec['total'] > 0 ? round($dcTotalOrderElec['list'][0]['sumElecQuantity'] / 1000, 3) : 0;
        $dcTotalElec = $dcTotalOrderElec['total'] > 0 ? (int)ceil($dcTotalOrderElec['list'][0]['sumElecQuantity'] / 1000) : 0;//直流总电量（kw/h）（进一取整）
        unset($orderData['minElecQuantity']);
        unset($orderData['deviceIds']);

        
        $orderData['minElecCostFinal'] = 0;
        $orderData['select'] = ['sumElecCostFinal' => 1];
        $orderElecPay = $this->getChargeOrderDB($orderData);
        $totalElecPay = $orderElecPay['total'] > 0 ? AccountHelper::transPayment($orderElecPay['list'][0]['sumElecCostFinal'], $currencyDest, 0, 1) : 0;//电费总花费（元）（一位小数）
        unset($orderData['minElecCostFinal']);
        
        $orderData['minServeCostFinal'] = 0;
        $orderData['select'] = ['sumServeCostFinal' => 1];
        $orderServePay = $this->getChargeOrderDB($orderData);
        $totalServePay = $orderServePay['total'] > 0 ? AccountHelper::transPayment($orderServePay['list'][0]['sumServeCostFinal'], $currencyDest, 0, 1) : 0;//服务费总花费（元）（一位小数）
        unset($orderData['minServeCostFinal']);
        
//        $orderData['minPayment'] = 0;
//        $orderData['select'] = ['sumPayment' => 1];
//        $orderPay = $this->getChargeOrderDB($orderData);
//        $totalPay = $orderPay['total'] > 0 ? AccountHelper::transPayment($orderPay['list'][0]['sumPayment'], $currencyDest, 0, 1) : 0;
//        unset($orderData['minPayment']);
        $totalPay = $totalElecPay + $totalServePay;//总花费（元）（一位小数）
        
        $result = [
            'totalCount' => $totalCount,//总订单数
            'orderUserCnt' => $orderUserCnt,//服务车主数
            'totalDuration' => $totalDura,//总时长
            'totalElecQuantity' => $totalElec,//总电量
            'dcElecQuanlity' => $dcTotalElec,//直流桩总电量
            'totalPayment' => $totalPay,//总收入
            'totalElecPay' => $totalElecPay,//总电费
            'totalServePay' => $totalServePay//总服务费
        ];
        
        return $result;        
    }
    
    /**
     * 开票处理
     * @param $data
     * @return array
     */
    public function invoice($data)
    {
        $result = [
            'msg' => ''
        ];

        $entity = [
            'orderNumbers' => $data['orderNumbers'],
        ];

        //调用请求
        $msg = $this->reqInvoice($entity);
        $result['msg'] = $msg;

        return $result;
    }

    /**
     * 开票操作
     * @param $data
     * @return string
     */
    private function reqInvoice($data)
    {
        $msg = '';
        $url = env('ABB_API_CHAGING', 'http://10.0.1.4') . ':18765/chargeOrder/order/v1/offlineInvoiceSign';
        $info = [
            'orderNumbers' => isset($data['orderNumbers']) ? (array)$data['orderNumbers'] : [],//订单编号的数组
        ];
        if (empty($info['orderNumbers'])) {
            return $msg = 'content_format';
        }
        $res = ChargeHelper::reqCharge($url, $info, 'ChargeOrder', 'reqInvoice');
        if ($res['code'] == 1) {
            return $msg = $res['msg'];
        } else {
            if (isset($res['body']['Ret']) && $res['body']['Ret'] == 1) {
                return $msg = $res['body']['Msg'];
            }
        }
        return $msg;
    }
    
    /**
     * 根据订单号获取订单的支付渠道金额明细
     *
     * @param string $orderNumber   订单号  因为有占位费订单，使用订单号避免重复
     * @param array $expend   扩展的查询参数 主要用于索引应用
     * @param array $select   需要查询的值
     * @return void
     */
    public static function getOrderPayChannelDetailByOrderNumber($orderNumber, $expend=[], $select=['balance_virtual_pay', 'balance_offline_pay','balance_wechat_pay']) {
        $strame = DB::table('balance_stream')->where('is_del', 0)->where('order_number', $orderNumber)->select($select);
        foreach($expend as $ek=>$ev) {
            $strame->where($ek, $ev);
        }
        $res = $strame->first();
        foreach($select as $sv) {
            $result[] = FeeHelper::formatTwo(empty($res)?0:FeeHelper::liUnitToYuan(abs($res[$sv])));
        }
        
        return $result;
    }
}
