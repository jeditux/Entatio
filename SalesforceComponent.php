<?php
namespace App\Controller\Component;

use App\Model\Entity\Course;
use App\Model\Entity\Credential;
use App\Model\Entity\Section;
use App\Model\Entity\User;
use Cake\Controller\Component;
use Cake\I18n\Time;
use Cake\ORM\TableRegistry;
use Cake\Utility\Hash;
use SforcePartnerClient;
use SObject;

/**
 * Class SalesforceComponent
 * @package App\Controller\Component
 * @property \App\Controller\Component\LogUtilsComponent $LogUtils
 */
class SalesforceComponent extends Component {
    public $components = array('LogUtils');

    const SF_PREFIX = 'KMTMMP__';
    const BATCH_SIZE = 200;
    const INSERT_COMMAND = 0;
    const UPDATE_COMMAND = 1;

    private function sendToSalesforce($command, $data, $toSObjectFunc) {
        $mySforceConnection = $this->connectToSalesForce();
        if($mySforceConnection) {
            try {
                $result = [];
                for ($startIndex = 0; $startIndex < count($data); $startIndex += self::BATCH_SIZE) {
                    $sObjects = [];
                    $finIndex = ($startIndex + self::BATCH_SIZE < count($data) ? $startIndex + self::BATCH_SIZE : count($data));
                    for ($i = $startIndex; $i < $finIndex; $i++) {
                        $item = $data[$i];
                        $sObject = $toSObjectFunc($item);
                        $sObjects[] = $sObject;
                    }
                    $response = ($command == self::INSERT_COMMAND ? $mySforceConnection->create($sObjects) : $mySforceConnection->update($sObjects));
                    $result = array_merge($result, $response);
                }
                $this->addErrorLogs($result, $data);
                return $result;
            } catch (\Exception $e) {
                $this->LogUtils->addLog([
                    'type' => LogUtilsComponent::WARNING,
                    'target' => 'sf',
                    'message' => $e->getMessage()
                ]);
                $this->log($e->getMessage());
            }
        }
        return false;
    }

    private function addErrorLogs($result, $sobjects = null){
        if(!$result){
            return;
        }
        foreach($result as $item){
            if(!$item->success){
                $error = 'Unexpected error occurred';
                if($item->errors){
                    $error = implode('; ', Hash::extract(json_decode(json_encode($item->errors), true), '{n}.message'));
                }
                if($sobjects){
                    $error = $error . '<hidden>' . json_encode($sobjects) . '</hidden>';
                }
                $this->LogUtils->addLog([
                    'type' => LogUtilsComponent::WARNING,
                    'target' => 'sf',
                    'message' => $error
                ]);
            }
        }
    }

    /**
     * @param $media
     * @param $section
     * @return bool
     */
    public function addActivity($medias){
        return $this->sendToSalesforce(self::INSERT_COMMAND, $medias, function($media) {
            $fields = array(
                self::SF_PREFIX . 'Name__c' => $media->name,
                'Name' => $media->name,
                self::SF_PREFIX . 'Description__c' => $media->description,
                self::SF_PREFIX . 'MM_Id__c' => $media->id,
                self::SF_PREFIX . 'Section__c' => $media->section->sf_id,
                self::SF_PREFIX . 'General__c' => $media->course->sf_id,
                self::SF_PREFIX . 'Visible__c' => true
            );
            $sObject = new SObject();
            $sObject->fields = $fields;
            $sObject->type = self::SF_PREFIX . 'Activity__c';
            return $sObject;
        });
    }

    public function setActivityInactive($sf_ids) {
        if(!is_array($sf_ids)){
            $sf_ids = [$sf_ids];
        }
        $mySforceConnection = $this->connectToSalesForce();
        if ($mySforceConnection) {
            try {
                $sObjectArr = [];
                foreach ($sf_ids as $sf_id) {
                    $fields = array(
                        'Id' => $sf_id,
                        self::SF_PREFIX . 'Inactive__c' => true
                    );
                    $sObject = new SObject();
                    $sObject->fields = $fields;
                    $sObject->type = self::SF_PREFIX . 'Activity__c';
                    $sObjectArr[] = $sObject;
                }
                $mySforceConnection->update($sObjectArr);
            } catch (\Exception $e) {
                $this->log($e->getMessage());
            }
        }
    }

    /**
     * Create Course in SF
     * @param $course Course
     * @return string Id of Course from SF
     */
    public function addCourse($course){
        $mySforceConnection = $this->connectToSalesForce();
        if ($mySforceConnection) {
            try {
                $presentationTable = TableRegistry::get('Presentation');
                if ($course->presentation_id != 0) {
                    $presentation = $presentationTable->get($course->presentation_id);
                } else {
                    $presentation = null;
                }
                $fields = array(
                    'Name' => $course->name,
                    self::SF_PREFIX . 'Connection_String__c' => $this->getConnectionStringId(),
                    self::SF_PREFIX . 'Course_Name__c' => $course->name,
                    self::SF_PREFIX . 'Course_Link__c' => $course->getLink(),
                    self::SF_PREFIX . 'Moodle_Course_Id__c' => $course->presentation_id,
                    self::SF_PREFIX . 'Description__c' => $presentation ? $presentation->description : ''
                );

                $sObject = new SObject();
                $sObject->fields = $fields;
                $sObject->type = self::SF_PREFIX . 'Course__c';

                $createResponse = $mySforceConnection->create(array($sObject));
                return $createResponse[0]->success ? $createResponse[0]->id : false;
            } catch (\Exception $e) {
                $this->log($e->getMessage());
            }
        }
        return false;
    }


    /**
     * Create Course in SF
     * @param $course Course
     * @return string Id of Course from SF
     */
    public function addAssigned($assignedList, $deleteDuplicates = false)
    {
        $connectionString = $this->getConnectionStringId();

        $assignedList = $this->checkAssignedEntatio($assignedList, $connectionString);

        if($deleteDuplicates){
            $activitySFIds = [];
            $attendeeSFIds = [];
            $entatioSFIds = [];
            foreach($assignedList as $assignedItem){
                $activitySFIds[] = $assignedItem->activity_sf_id;
                $attendeeSFIds[] = $assignedItem->user_sf_id;
                $entatioSFIds[] = $assignedItem->entatio_sf_id;
            }
            $dataForDelete = new \stdClass();
            $dataForDelete->activitySFIds = $activitySFIds;
            $dataForDelete->attendeeSFIds = $attendeeSFIds;
            $dataForDelete->entatioSFIds = $entatioSFIds;
            $this->deleteAssigned($dataForDelete);
        }

        return $this->sendToSalesforce(self::INSERT_COMMAND, $assignedList, function($assignedItem) use ($connectionString){
            $fields = [
                self::SF_PREFIX . 'Entatio__c' => $assignedItem->entatio_sf_id,
                //self::SF_PREFIX . 'Course_MM_Id__c' => $assignedItem->entatio_id,
                self::SF_PREFIX . 'Activity__c' => $assignedItem->activity_sf_id,
                self::SF_PREFIX . 'User__c' => $assignedItem->user_sf_id,
                self::SF_PREFIX . 'Assigned__c' => $assignedItem->entatio_assigned ? 1 : 0,
                self::SF_PREFIX . 'Connection_String__c' => $connectionString,
            ];
            if($assignedItem->assigned_date){
                $fields[self::SF_PREFIX . 'AssignedDate__c'] = $assignedItem->assigned_date;
            }
            $sObject = new SObject();
            $sObject->fields = $fields;
            $sObject->type = self::SF_PREFIX . 'Assigned__c';
            return $sObject;
        });
    }

    public function checkAssignedEntatio($assignedList, $connectionString){

        //get all entatios with empty sf_id field
        $entatioForAdd = [];
        foreach($assignedList as $assignedItem){
            if($assignedItem->entatio_sf_id || $entatioForAdd[$assignedItem->entatio_id]){
                continue;
            }
            $entatio = new \stdClass();
            $entatio->name = htmlspecialchars($assignedItem->entatio_name);
            $entatio->id = $assignedItem->entatio_id;
            $entatioForAdd[$assignedItem->entatio_id] = $entatio;
        }

        //no entatios for saving in sf
        if(count($entatioForAdd) == 0){
            return $assignedList;
        }


        $entatioIds = array_keys($entatioForAdd);
        $entatioTable = TableRegistry::get('Entatio');
        $entatios = $entatioTable->find()->where(['id IN' => $entatioIds])->all()->toArray();
        $entatiosWithSfId = $this->addEntatio($entatios, false, true);

        //check sf saving status and create array of Entatios with sf_id field for saving in DB
        if ($entatiosWithSfId) {
            $entatioIdToSfIdMap = [];
            foreach($entatiosWithSfId as $entatioWitSfId){
                if ($entatioWitSfId['sf_id']) {
                    $entatioIdToSfIdMap[$entatioWitSfId['id']] = $entatioWitSfId['sf_id'];
                }
            }
            foreach ($assignedList as $assigned) {
                if ($entatioIdToSfIdMap[$assigned->entatio_id]) {
                    $assigned->entatio_sf_id = $entatioIdToSfIdMap[$assigned->entatio_id];
                }
            }
        }

        return $assignedList;
    }

    /**
     * Add Entatio in SF
     * @param $entatios array of Entatio objects
     * @param bool $resetSFId create new sf object and overwrite sf id (for initial sync)
     * @return bool
     */
    public function addEntatio($entatios, $resetSFId = false, $isArray = false){

        //conver single object to array
        if(!$isArray){
            $entatios = [$entatios];
        }

        //create maps for add in sf and for save in db
        $entatiosMap = [];
        $entatioForAddInSF = [];
        foreach($entatios as $entatio){
            //skip if alread in list for add or if already added in salesforce and not need to overwrite
            if($entatiosMap[$entatio->id] || ($entatio->sf_id && !$resetSFId)){
                continue;
            }
            $entatioObject = new \stdClass();
            $entatioObject->name = htmlspecialchars($entatio->name);
            $entatioObject->id = $entatio->id;
            $entatioForAddInSF[] = $entatioObject;
            $entatiosMap[$entatio->id] = $entatio;
        }

        if(count($entatioForAddInSF) == 0){
            return false;
        }

        $connectionString = $this->getConnectionStringId();
        $entatioForAddInSFValues = array_values($entatioForAddInSF);

        //save objects in sf
        $entatioListResult = $this->sendToSalesforce(self::INSERT_COMMAND, $entatioForAddInSFValues, function($e) use ($connectionString){
            $fields = [
                'Name' => $e->name,
            ];
            $sObject = new SObject();
            $sObject->fields = $fields;
            $sObject->type = self::SF_PREFIX . 'Entatio__c';
            return $sObject;
        });

        //check sf saving status and create array of Entatios with sf_id field for saving in DB
        if ($entatioListResult) {
            foreach ($entatioForAddInSF as $index => $entatioObject) {
                if (!$entatioListResult[$index]->success || !$entatiosMap[$entatioObject->id]) {
                    continue;
                }
                $entatioForSave = $entatiosMap[$entatioObject->id];
                $entatioForSave->sf_id = $entatioListResult[$index]->id;
                $entatioForUpdateSfId[] = $entatioForSave;
            }
        }

        if(count($entatioForUpdateSfId) > 0){
            $entatioTable = TableRegistry::get('Entatio');
            $entatios = $entatioTable->saveMany($entatioForUpdateSfId);
        }
        return $entatios;
    }

    public function setEntatioInactive($entatioIds){

        if(count($entatioIds) == 0){
            return false;
        }

        $entatioTable = TableRegistry::get('Entatio');
        $entatios = $entatioTable->find()->where(['id IN' => $entatioIds, 'sf_id IS NOT NULL'])->all()->toArray();

        return $this->sendToSalesforce(self::UPDATE_COMMAND, $entatios, function($e){
            $fields = [
                'Id' => $e['sf_id'],
                self::SF_PREFIX . 'Inactive__c' => 'TRUE'
            ];
            $sObject = new SObject();
            $sObject->fields = $fields;
            $sObject->type = self::SF_PREFIX . 'Entatio__c';
            return $sObject;
        });
    }

    public function setAssignedInactive($assignedSfIds){
        if(!$assignedSfIds){
            return null;
        }
        return $this->sendToSalesforce(self::UPDATE_COMMAND, $assignedSfIds, function($e){
            $fields = [
                'Id' => $e['sf_id'],
                self::SF_PREFIX . 'Inactive__c' => 'TRUE'
            ];
            $sObject = new SObject();
            $sObject->fields = $fields;
            $sObject->type = self::SF_PREFIX . 'Assigned__c';
            return $sObject;
        });
    }

    public function setAssignedUnassigned($data = null)
    {
        if (!$data) {
            return;
        }
        $whereDelete = [];
        $connectionString = $this->getConnectionStringId();

        foreach ($data as $s) {
            $whereDelete[] = '(' .
                self::SF_PREFIX . 'Activity__c IN (\'' . implode('\', \'', $s->activitySFIds) . '\') AND ' .
                self::SF_PREFIX . 'User__c = \'' . $s->userSFId . '\' AND ' .
                self::SF_PREFIX . 'Entatio__c = \'' . $s->entatioSFId . '\') ';
        }
        $whereString = ' WHERE ' . self::SF_PREFIX . 'Connection_String__c = \'' . $connectionString . '\' AND (' . implode(' OR ', $whereDelete) . ')';
        $assignedSfIds = $this->getAssignedList($whereString);

        if (!$assignedSfIds) {
            return null;
        }
        return $this->sendToSalesforce(self::UPDATE_COMMAND, $assignedSfIds, function ($e) {
            $fields = [
                'Id' => $e['sf_id'],
                self::SF_PREFIX . 'Assigned__c' => 'FALSE'
            ];
            $sObject = new SObject();
            $sObject->fields = $fields;
            $sObject->type = self::SF_PREFIX . 'Assigned__c';
            return $sObject;
        });
    }

    public function changeAssignedEntatioName($entatio)
    {
        if(!$entatio->sf_id){
            return null;
        }
        $connectionString = $this->getConnectionStringId();
        return $this->sendToSalesforce(self::UPDATE_COMMAND, [$entatio], function($e) use ($connectionString){
            $fields = [
                'Name' => htmlspecialchars($e->name),
                'Id' => $e->sf_id
            ];
            $sObject = new SObject();
            $sObject->fields = $fields;
            $sObject->type = self::SF_PREFIX . 'Entatio__c';
            return $sObject;
        });
    }

    public function deleteAssigned($data = null){
        $whereDelete = [];
        $connectionString = $this->getConnectionStringId();
        if($data) {
            $activitySFIds = $data->activitySFIds;
            $attendeeSFIds = $data->attendeeSFIds;
            $entatioSFIds = $data->entatioSFIds;
            if (count($activitySFIds) > 0) {
                $whereDelete[] = self::SF_PREFIX . 'Activity__c IN (\'' . implode('\', \'', $activitySFIds) . '\') ';
            }
            if (count($attendeeSFIds) > 0) {
                $whereDelete[] = self::SF_PREFIX . 'User__c IN (\'' . implode('\', \'', $attendeeSFIds) . '\') ';
            }
            if (count($entatioSFIds) > 0) {
                $whereDelete[] = self::SF_PREFIX . 'Entatio__c IN (\'' . implode('\', \'', $entatioSFIds) . '\') ';
            }
        }
        $whereDelete[] = self::SF_PREFIX . 'Connection_String__c = \'' . $connectionString . '\' ';
        $whereString = ' WHERE ' . implode(' AND ', $whereDelete);
        $sfIds = $this->getAssignedList($whereString);
        $this->setAssignedInactive($sfIds);
    }

    private function getAssignedList($whereString = null) {
        $mySforceConnection = $this->connectToSalesForce();
        if ($mySforceConnection) {
            try {
                $sql = 'SELECT Id, '.
                    self::SF_PREFIX . 'Connection_String__c, '.
                    self::SF_PREFIX . 'Entatio__c, '.
                    self::SF_PREFIX . 'Activity__c, '.
                    self::SF_PREFIX . 'User__c, '.
                    self::SF_PREFIX . 'Inactive__c FROM '.
                    self::SF_PREFIX . 'Assigned__c' . ($whereString ? $whereString . ' AND ' : ' WHERE ') .
                    self::SF_PREFIX . 'Inactive__c = FALSE';
                $queryResult = $mySforceConnection->query($sql);
            } catch (\Exception $e) {
                $this->log($e->getMessage());
                return false;
            }
            $result = array();
            for ($queryResult->rewind(); $queryResult->pointer < $queryResult->size; $queryResult->next()) {
                $sObj = $queryResult->current();
                $sfIds = [
                    'sf_id' => $sObj->Id,
                ];
                $result[] = $sfIds;
            }
            return $result;
        }
    }

    public function updateCourse($course) {
        $mySforceConnection = $this->connectToSalesForce();
        if ($mySforceConnection) {
            try {
                $presentationTable = TableRegistry::get('Presentation');
                $presentation = $presentationTable->get($course->presentation_id);
                if ($presentation) {
                    $fields = array(
                        'Id' => $course->sf_id,
                        'Name' => $course->name,
                        self::SF_PREFIX . 'Connection_String__c' => $this->getConnectionStringId(),
                        self::SF_PREFIX . 'Course_Name__c' => $course->name,
                        self::SF_PREFIX . 'Course_Link__c' => $course->getLink(),
                        self::SF_PREFIX . 'Moodle_Course_Id__c' => $course->presentation_id,
                        self::SF_PREFIX . 'Description__c' => $presentation->description
                    );

                    $sObject = new SObject();
                    $sObject->fields = $fields;
                    $sObject->type = self::SF_PREFIX . 'Course__c';

                    $updateResponse = $mySforceConnection->update(array($sObject));
                    return $updateResponse[0]->success ? $updateResponse[0]->id : false;
                }
            } catch (\Exception $e) {
                $this->log($e->getMessage());
            }
        }
        return false;
    }

    /**
     * Create Section in SF
     * @param $section Section Local Section
     * @param $course Course Local Course
     * @return string Id of Section from SF
     */
    public function addSection($section, $course){
        $mySforceConnection = $this->connectToSalesForce();
        if ($mySforceConnection) {
            try {
                $fields = array(
                    'Name' => $section->name,
                    self::SF_PREFIX . 'Name__c' => $section->name,
                    self::SF_PREFIX . 'MM_Id__c' => $section->id,
                    self::SF_PREFIX . 'Course__c' => $course->sf_id
                );

                $sObject = new SObject();
                $sObject->fields = $fields;
                $sObject->type = self::SF_PREFIX . 'Section__c';

                $createResponse = $mySforceConnection->create(array($sObject));

                return $createResponse[0]->success ? $createResponse[0]->id : false;
            } catch (\Exception $e) {
                $this->log($e->getMessage());
            }
        }
        return false;
    }

    public function updateSection($section) {
        $mySforceConnection = $this->connectToSalesForce();
        if ($mySforceConnection) {
            try {
                $fields = array(
                    'Id' => $section->sf_id,
                    'Name' => $section->name,
                    self::SF_PREFIX . 'Name__c' => $section->name,
                );

                $sObject = new SObject();
                $sObject->fields = $fields;
                $sObject->type = self::SF_PREFIX . 'Section__c';

                $updateResponse = $mySforceConnection->update(array($sObject));
                return $updateResponse[0]->success ? $updateResponse[0]->id : false;
            } catch (\Exception $e) {
                $this->log($e->getMessage());
            }
        }
        return false;
    }

    public function setCourseInactive($sf_id) {
        $mySforceConnection = $this->connectToSalesForce();
        if ($mySforceConnection) {
            try {
                $fields = array(
                    'Id' => $sf_id,
                    self::SF_PREFIX . 'Inactive__c' => true
                );
                $sObject = new SObject();
                $sObject->fields = $fields;
                $sObject->type = self::SF_PREFIX . 'Course__c';
                $mySforceConnection->update(array($sObject));
            } catch (\Exception $e) {
                $this->log($e->getMessage());
            }
        }
    }

    public function setCoursesInactive($sf_ids) {
        return $this->sendToSalesforce(self::UPDATE_COMMAND, $sf_ids, function ($sf_id) {
            $fields = array(
                'Id' => $sf_id,
                self::SF_PREFIX . 'Inactive__c' => true
            );
            $sObject = new SObject();
            $sObject->fields = $fields;
            $sObject->type = self::SF_PREFIX . 'Course__c';
            return $sObject;
        });
    }

    /**
     * @param User $user
     */
    public function addUser($users){
        return $this->sendToSalesforce(self::INSERT_COMMAND, $users, function($user) {
            $connectionString = $this->getConnectionStringId();
            $fields = array(
                'Name' => ($user->first_name && $user->last_name ? $user->first_name . ' ' . $user->last_name : $user->email),
                self::SF_PREFIX . 'Firstname__c' => $user->first_name,
                self::SF_PREFIX . 'Lastname__c' => $user->last_name,
                self::SF_PREFIX . 'Phone1__c' => $user->phone,
                self::SF_PREFIX . 'Username__c' => $user->email,
                self::SF_PREFIX . 'MM_Id__c' => $user->user_id,
                self::SF_PREFIX . 'Connection_String__c' => $connectionString,
            );

            $sObject = new SObject();
            $sObject->fields = $fields;
            $sObject->type = self::SF_PREFIX . 'MM_User__c';
            return $sObject;
        });
    }

    public function updateUser($users) {
        return $this->sendToSalesforce(self::UPDATE_COMMAND, $users, function($user) {
            $fields = array(
                'Id' => $user->sf_id,
                'Name' => ($user->first_name && $user->last_name ? $user->first_name . ' ' . $user->last_name : $user->email),
                self::SF_PREFIX . 'Firstname__c' => $user->first_name,
                self::SF_PREFIX . 'Lastname__c' => $user->last_name,
                self::SF_PREFIX . 'Phone1__c' => $user->phone,
                self::SF_PREFIX . 'Username__c' => $user->email,
            );

            $sObject = new SObject();
            $sObject->fields = $fields;
            $sObject->type = self::SF_PREFIX . 'MM_User__c';
            return $sObject;
        });
    }

    public function setUserInactive($sf_id)
    {
        $mySforceConnection = $this->connectToSalesForce();
        if ($mySforceConnection) {
            try {
                $fields = array(
                    'Id' => $sf_id,
                    self::SF_PREFIX . 'Inactive__c' => true
                );
                $sObject = new SObject();
                $sObject->fields = $fields;
                $sObject->type = self::SF_PREFIX . 'MM_User__c';
                $mySforceConnection->update(array($sObject));
            } catch (\Exception $e) {
                $this->log($e->getMessage());
            }
        }
    }

    public function addKeyword($keywords) {
        return $this->sendToSalesforce(self::INSERT_COMMAND, $keywords, function($keyword) {
            $fields = array(
                'Name' => ($keyword->name),
                self::SF_PREFIX . 'MM_Id__c' => $keyword->id
            );

            $sObject = new SObject();
            $sObject->fields = $fields;
            $sObject->type = self::SF_PREFIX . 'Keyword__c';
            return $sObject;
        });
    }

    public function addKeywordBinding($keywordBindings) {
        return $this->sendToSalesforce(self::INSERT_COMMAND, $keywordBindings, function($kb) {
            $fields = array(
                self::SF_PREFIX . 'Keyword__c' => $kb['keyword'],
                self::SF_PREFIX . 'Interaction_Activity__c' => $kb['activity'],
                self::SF_PREFIX . 'Presentation__c' => $kb['presentation']
            );

            $sObject = new SObject();
            $sObject->fields = $fields;
            $sObject->type = self::SF_PREFIX . 'Keyword_Binding__c';
            return $sObject;
        });
    }

    public function setKeywordInactive($sf_ids) {
        return $this->sendToSalesforce(self::UPDATE_COMMAND, $sf_ids, function($sf_id) {
            $fields = array(
                'Id' => $sf_id,
                self::SF_PREFIX . 'Inactive__c' => true
            );

            $sObject = new SObject();
            $sObject->fields = $fields;
            $sObject->type = self::SF_PREFIX . 'Keyword__c';
            return $sObject;
        });
    }

    public function setKeywordBindingInactive($sf_ids) {
        return $this->sendToSalesforce(self::UPDATE_COMMAND, $sf_ids, function($sf_id) {
            $fields = array(
                'Id' => $sf_id,
                self::SF_PREFIX . 'Inactive__c' => true
            );

            $sObject = new SObject();
            $sObject->fields = $fields;
            $sObject->type = self::SF_PREFIX . 'Keyword_Binding__c';
            return $sObject;
        });
    }

    public function updateActivityName($data) {
        return $this->sendToSalesforce(self::UPDATE_COMMAND, $data, function($activity) {
            $fields = array(
                'Id' => $activity->sf_id,
                'Name' => $activity->name,
                self::SF_PREFIX . 'Name__c' => $activity->name,
            );

            $sObject = new SObject();
            $sObject->fields = $fields;
            $sObject->type = self::SF_PREFIX . 'Activity__c';
            return $sObject;
        });
    }

    public function addConnectionString($connectionString, $credentials = false)
    {
        $mySforceConnection = $this->connectToSalesForce($credentials);
        if ($mySforceConnection) {
            try {
                $queryResult = $mySforceConnection->query('SELECT Id, Name FROM ' . self::SF_PREFIX . 'Connection_String__c WHERE ' . self::SF_PREFIX . 'Url__c=\'' . $connectionString->url . '\' LIMIT 1');
            } catch (\Exception $e) {
                return false;
            }
            if (count($queryResult->records) == 0) {
                $fields = array(
                    'Name' => $connectionString->name,
                    self::SF_PREFIX . 'Name__c' => $connectionString->name,
                    self::SF_PREFIX . 'Url__c' => $connectionString->url
                );

                $sObject = new SObject();
                $sObject->fields = $fields;
                $sObject->type = self::SF_PREFIX . 'Connection_String__c';

                try {
                    $createResponse = $mySforceConnection->create(array($sObject));
                } catch (\Exception $e) {
                    return false;
                }

                return $createResponse[0]->success ? $createResponse[0]->id : false;
            } else {
                return $queryResult->records[0]->Id[0];
            }
        }
        return false;
    }

    /**
     * @param $completion array
     */
    public function addCompletion($completions){
        return $this->sendToSalesforce(self::INSERT_COMMAND, $completions, function($completion) {
            $fields = array(
                self::SF_PREFIX . 'Activity__c' => $completion['activity'],
                self::SF_PREFIX . 'Completed__c' => $completion['completed'],
                self::SF_PREFIX . 'Media_Manager_User__c' => $completion['attendee'],
                self::SF_PREFIX . 'Inactive__c' => $completion['inactive'],
                self::SF_PREFIX . 'MM_Id__c' => $completion['MM_Id'],
            );

            if($completion['completed_date']){
                $fields[self::SF_PREFIX . 'CompletedDate__c'] = $completion['completed_date'];
            }

            $sObject = new SObject();
            $sObject->fields = $fields;
            $sObject->type = self::SF_PREFIX . 'Activity_Completion__c';
            return $sObject;
        });
    }

    public function setCompleted($completions) {
        return $this->sendToSalesforce(self::UPDATE_COMMAND, $completions, function($completion) {
            $fields = array(
                'Id' => $completion->sf_id,
                self::SF_PREFIX . 'Completed__c' => $completion->completed ? 1 : 0,
            );
            if($completion->completed){
                $date = date(DATE_ATOM, $completion->completed_at && $completion->completed_at->i18nFormat ? $completion->completed_at->i18nFormat(Time::UNIX_TIMESTAMP_FORMAT) : time());
                $fields[self::SF_PREFIX . 'CompletedDate__c'] = $date;
            }
            if($completion->inactive){
                $fields[self::SF_PREFIX . 'Inactive__c'] = true;
            }
            $sObject = new SObject();
            $sObject->fields = $fields;
            $sObject->type = self::SF_PREFIX . 'Activity_Completion__c';
            return $sObject;
        });
    }

    public function setCompletionStatus($completions/*, $status*/) {
        return $this->sendToSalesforce(self::UPDATE_COMMAND, $completions, function($completion) {
            $fields = array(
                'Id' => $completion->sf_id,
                self::SF_PREFIX . 'Inactive__c' => $completion->status
            );
            $sObject = new SObject();
            $sObject->fields = $fields;
            $sObject->type = self::SF_PREFIX . 'Activity_Completion__c';
            return $sObject;
        });
    }

    public function setCompletionInactive($sf_id) {
        $mySforceConnection = $this->connectToSalesForce();
        if ($mySforceConnection) {
            try {
                $fields = array(
                    'Id' => $sf_id,
                    self::SF_PREFIX . 'Inactive__c' => true
                );
                $sObject = new SObject();
                $sObject->fields = $fields;
                $sObject->type = self::SF_PREFIX . 'Activity_Completion__c';
                $mySforceConnection->update(array($sObject));
            } catch (\Exception $e) {
                $this->log($e->getMessage());
            }
        }
    }

    public function setCompletionsInactive($sf_ids) {
        return $this->sendToSalesforce(self::UPDATE_COMMAND, $sf_ids, function ($sf_id) {
            $fields = array(
                'Id' => $sf_id,
                self::SF_PREFIX . 'Inactive__c' => true
            );
            $sObject = new SObject();
            $sObject->fields = $fields;
            $sObject->type = self::SF_PREFIX . 'Activity_Completion__c';
            return $sObject;
        });
    }

    public function getUserList()
    {
        $mySforceConnection = $this->connectToSalesForce();
        $csTable = TableRegistry::get('ConnectionString');
        $connectionString = $csTable->find()->first();
        if ($mySforceConnection) {
            try {
                $queryResult = $mySforceConnection->query('SELECT Id, ' . self::SF_PREFIX . 'MM_Id__c FROM ' . self::SF_PREFIX . 'MM_User__c WHERE ' . self::SF_PREFIX . 'Connection_String__c=\'' . $connectionString->sf_id . '\'');
            } catch (\Exception $e) {
                $this->log($e->getMessage());
                return false;
            }
            $result = array();
            for ($queryResult->rewind(); $queryResult->pointer < $queryResult->size; $queryResult->next()) {
                $sfUser = new \stdClass();
                $sObj = $queryResult->current();
                $sfUser->sf_id = $sObj->Id;
                $sfUser->MM_Id = $sObj->fields->{self::SF_PREFIX . 'MM_Id__c'};
                $result[] = $sfUser;
            }
            return $result;
        }
    }

    public function getPresentationList()
    {
        $mySforceConnection = $this->connectToSalesForce();
        $csTable = TableRegistry::get('ConnectionString');
        $connectionString = $csTable->find()->first();
        if ($mySforceConnection) {
            try {
                $queryResult = $mySforceConnection->query('SELECT Id, ' . self::SF_PREFIX . 'Moodle_Course_Id__c FROM ' . self::SF_PREFIX . 'Course__c WHERE ' . self::SF_PREFIX . 'Connection_String__c=\'' . $connectionString->sf_id . '\'');
            } catch (\Exception $e) {
                $this->log($e->getMessage());
                return false;
            }
            $result = array();
            for ($queryResult->rewind(); $queryResult->pointer < $queryResult->size; $queryResult->next()) {
                $sfPresentation = new \stdClass();
                $sObj = $queryResult->current();
                $sfPresentation->sf_id = $sObj->Id;
                $sfPresentation->MM_Id = $sObj->fields->{self::SF_PREFIX . 'Moodle_Course_Id__c'};
                $result[] = $sfPresentation;
            }
            return $result;
        }
    }

    public function getSectionList($courseId)
    {
        $mySforceConnection = $this->connectToSalesForce();
        if ($mySforceConnection) {
            try {
                $queryResult = $mySforceConnection->query('SELECT Id,Name,' . self::SF_PREFIX . 'MM_Id__c FROM ' . self::SF_PREFIX . 'Section__c WHERE ' . self::SF_PREFIX . 'Course__c=\'' . $courseId . '\'');
            } catch (\Exception $e) {
                $this->log($e->getMessage());
                return false;
            }
            $result = array();
            for ($queryResult->rewind(); $queryResult->pointer < $queryResult->size; $queryResult->next()) {
                $sfSection = new \stdClass();
                $sObj = $queryResult->current();
                $sfSection->sf_id = $sObj->Id;
                $sfSection->MM_Id = $sObj->fields->{self::SF_PREFIX . 'MM_Id__c'};
                $sfSection->Name = $sObj->fields->Name;
                $result[] = $sfSection;
            }
            return $result;
        }
    }

    public function getActivityList($courseId)
    {
        $mySforceConnection = $this->connectToSalesForce();
        if ($mySforceConnection) {
            try {
                $queryResult = $mySforceConnection->query('SELECT Id,Name,' . self::SF_PREFIX . 'MM_Id__c FROM ' . self::SF_PREFIX . 'Activity__c WHERE ' . self::SF_PREFIX . 'General__c=\'' . $courseId . '\'');
            } catch (\Exception $e) {
                $this->log($e->getMessage());
                return false;
            }
            $result = array();
            for ($queryResult->rewind(); $queryResult->pointer < $queryResult->size; $queryResult->next()) {
                $sfActivity = new \stdClass();
                $sObj = $queryResult->current();
                $sfActivity->sf_id = $sObj->Id;
                $sfActivity->MM_Id = $sObj->fields->{self::SF_PREFIX . 'MM_Id__c'};
                $sfActivity->Name = $sObj->fields->Name;
                $result[] = $sfActivity;
            }
            return $result;
        }
    }

    public function getCompletionList($activityId)
    {
        $mySforceConnection = $this->connectToSalesForce();
        if ($mySforceConnection) {
            try {
                $queryResult = $mySforceConnection->query('SELECT Id,' . self::SF_PREFIX . 'MM_Id__c,' . self::SF_PREFIX . 'Media_Manager_User__c,' . self::SF_PREFIX . 'Inactive__c FROM ' . self::SF_PREFIX . 'Activity_Completion__c WHERE ' . self::SF_PREFIX . 'Activity__c=\'' . $activityId . '\'');
            } catch (\Exception $e) {
                $this->log($e->getMessage());
                return false;
            }
            $result = array();
            for ($queryResult->rewind(); $queryResult->pointer < $queryResult->size; $queryResult->next()) {
                $sfCompletion = new \stdClass();
                $sObj = $queryResult->current();
                $sfCompletion->sf_id = $sObj->Id;
                $sfCompletion->MM_Id = $sObj->fields->{self::SF_PREFIX . 'MM_Id__c'};
                $sfCompletion->MM_User = $sObj->fields->{self::SF_PREFIX . 'Media_Manager_User__c'};
                $sfCompletion->Activity = $activityId;
                $sfCompletion->Inactive = $sObj->{self::SF_PREFIX . 'Inactive__c'};
                $result[] = $sfCompletion;
            }
            return $result;
        }
    }

    public function getKeywordList() {
        $mySforceConnection = $this->connectToSalesForce();
        if ($mySforceConnection) {
            try {
                $queryResult = $mySforceConnection->query('SELECT Id,Name,' . self::SF_PREFIX . 'MM_Id__c FROM ' . self::SF_PREFIX . 'Keyword__c');
            } catch (\Exception $e) {
                $this->log($e->getMessage());
                return false;
            }
            $result = array();
            for ($queryResult->rewind(); $queryResult->pointer < $queryResult->size; $queryResult->next()) {
                $sObj = $queryResult->current();
                $sfKeyword = [
                    'sf_id' => $sObj->Id,
                    'Name' => $sObj->fields->Name,
                    'MM_Id' => $sObj->fields->{self::SF_PREFIX . 'MM_Id__c'}
                ];
                $result[] = $sfKeyword;
            }
            return $result;
        }
    }

    public function getKeywordBindingList() {
        $mySforceConnection = $this->connectToSalesForce();
        if ($mySforceConnection) {
            try {
                $queryResult = $mySforceConnection->query('SELECT Id,Name,' . self::SF_PREFIX . 'MM_Id__c,' . self::SF_PREFIX . '\'Interaction_Activity__c,\'' . self::SF_PREFIX . '\'Presentation__c,\'' . self::SF_PREFIX . '\'Inactive__c FROM ' . self::SF_PREFIX . 'Keyword_Binding__c');
            } catch (\Exception $e) {
                $this->log($e->getMessage());
                return false;
            }
            $result = array();
            for ($queryResult->rewind(); $queryResult->pointer < $queryResult->size; $queryResult->next()) {
                $sObj = $queryResult->current();
                $sfKeywordBinding = [
                    'sf_id' => $sObj->Id,
                    'Name' => $sObj->fields->Name,
                    'MM_Id' => $sObj->fields->{self::SF_PREFIX . 'MM_Id__c'},
                    'Activity' => $sObj->fields->{self::SF_PREFIX . 'Interaction_Activity__c'},
                    'Presentation' => $sObj->fields->{self::SF_PREFIX . 'Presentation__c'}
                ];
                $result[] = $sfKeywordBinding;
            }
            return $result;
        }
    }

    public function getFullCompletionList()
    {
        $mySforceConnection = $this->connectToSalesForce();
        if ($mySforceConnection) {
            try {
                $queryResult = $mySforceConnection->query('SELECT Id,' . self::SF_PREFIX . 'MM_Id__c,' . self::SF_PREFIX . 'Media_Manager_User__c,' . self::SF_PREFIX . 'Activity__c,' . self::SF_PREFIX . 'Inactive__c FROM ' . self::SF_PREFIX . 'Activity_Completion__c');
            } catch (\Exception $e) {
                $this->log($e->getMessage());
                return false;
            }
            $done = false;
            $result = array();
            while (!$done) {
                for ($queryResult->rewind(); $queryResult->pointer < $queryResult->size; $queryResult->next()) {
                    $sfCompletion = new \stdClass();
                    $sObj = $queryResult->current();
                    $sfCompletion->sf_id = $sObj->Id;
                    $sfCompletion->MM_Id = $sObj->fields->{self::SF_PREFIX . 'MM_Id__c'};
                    $sfCompletion->MM_User = $sObj->fields->{self::SF_PREFIX . 'Media_Manager_User__c'};
                    $sfCompletion->Activity = $sObj->fields->{self::SF_PREFIX . 'Activity__c'};
                    $sfCompletion->Inactive = $sObj->{self::SF_PREFIX . 'Inactive__c'};
                    $result[] = $sfCompletion;
                }
			    if ($queryResult->done) {
                    $done = true;
			    } else {
                    $queryResult = $mySforceConnection->queryMore($queryResult->queryLocator);
			    }
            }
            return $result;
        }
    }

    private function connectToSalesForce($credentials = false)
    {
        if (!ENABLE_COMPLETION_RECORDING && !$credentials) {
            return false;
        }

        $mySforceConnection = null;
        if (!$credentials) {
            $credentials = $this->getSFCredential();
        }
        if (!$credentials) {
            return false;
        }

        try {
            $slogin = $credentials->username;
            $spass = $credentials->password;
            $stoken = $credentials->token;

            $mySforceConnection = new SforcePartnerClient();
            $mySoapClient = $mySforceConnection->createConnection(USER_HOME_DIR . 'Lib/soapclient/partner.wsdl.xml');
            $mySforceConnection->setEndpoint('https://' . $credentials->host . '/services/Soap/u/27.0');

            if (true) {
                $mySforceConnection->login($slogin, $spass . $stoken);
            } else {
                $mySforceConnection->login($slogin, $spass);
            }

        } catch (\Exception $e) {
            $mySforceConnection = null;
        }
        return $mySforceConnection;
    }

    private function getSFCredential(){
        $credentialModel = TableRegistry::get('Credential');
        return $credentialModel->find()->where(['type' => Credential::TYPE_SALESFORCE])->first();
    }

    private function getConnectionStringId(){
        $csTable = TableRegistry::get('ConnectionString');
        $connectionString = $csTable->find()->first();
        return $connectionString->sf_id;
    }

    public function validateCredentials($credentials){
        return $this->connectToSalesForce($credentials) ? 'SUCCESS' : 'FAIL';
    }
}
