<?php
/**
 * Created by PhpStorm.
 * User: 1
 * Date: 13.02.2019
 * Time: 16:59
 */

namespace App\Controller\Component;


use Cake\Controller\Component;
use Cake\ORM\TableRegistry;

/**
 * Class KeywordUtilsComponent
 * @package App\Controller\Component
 * @property \App\Controller\Component\SalesforceComponent $Salesforce
 */

class KeywordUtilsComponent extends Component
{
    public $components = array('Salesforce');

    const MEDIA = 0;
    const PRESENTATION = 1;
    const CONVERSION_TASK = 2;

    public function getKeywordList($id, $type) {
        $keywordTable = TableRegistry::get('Keyword');
        $allKeywords = $keywordTable->find()->order(['name' => 'ASC'])->toArray();
        if ($type == self::MEDIA) {
            $entityKeywords = $keywordTable->find()
                ->join([
                    'table'=> 'keyword_binding',
                    'alias' => 'kb',
                    'type' => 'LEFT',
                    'conditions' => 'Keyword.id=kb.keyword_id'
                ])
                ->where(['media_id' => $id])->toArray();
        } elseif ($type == self::PRESENTATION) {
            $entityKeywords = $keywordTable->find()
                ->join([
                    'table'=> 'keyword_binding',
                    'alias' => 'kb',
                    'type' => 'LEFT',
                    'conditions' => 'Keyword.id=kb.keyword_id'
                ])
                ->where(['presentation_id' => $id])->toArray();
        }
        return ['allKeywords' => $allKeywords, 'entityKeywords' => $entityKeywords];
    }

    public function processKeywords($data, $entityId, $entityType) {
        $addList = array();
        $deleteList = array();
        foreach ($data as $item) {
            if ($item->toAdd) {
                $addList[] = $item->name;
            } elseif ($item->toDelete) {
                $deleteList[] = $item->name;
            }
        }
        if (count($addList) > 0) {
            $this->saveKeywordList($addList, $entityId, $entityType);
        }
        if (count($deleteList) > 0) {
            $this->deleteKeywordList($deleteList, $entityId, $entityType);
        }
    }

    private function saveKeywordList($list, $entityId, $entityType) {
        $keywordTable = TableRegistry::get('Keyword');
        $keywordBindingTable = TableRegistry::get('KeywordBinding');
        $existingKeywordIds = $keywordTable->find()->where(['name IN' => $list])->extract('id')->toArray();
        $existingKeywords = $keywordTable->find()->where(['name IN' => $list])->extract('name')->toArray();
        $newKeywords = count($existingKeywords) > 0 ? array_diff($list, $existingKeywords) : $list;
        $keywordsToSf = array();
        foreach ($newKeywords as $keyword) {
            $keywordItem = $keywordTable->newEntity(['name' => $keyword]);
            $keywordItem = $keywordTable->save($keywordItem);
            $existingKeywordIds[] = $keywordItem->id;
            $keywordsToSf[] = $keywordItem;
        }
        $sf_ids = $this->Salesforce->addKeyword($keywordsToSf);
        foreach ($keywordsToSf as $index => $keyword){
            if($sf_ids[$index] && $sf_ids[$index]->success) {
                $keyword->sf_id = $sf_ids[$index]->id;
            }
        }
        $success = $keywordTable->saveMany($keywordsToSf);

        switch ($entityType) {
        case self::MEDIA:
            $entityIdField = 'media_id';
            break;
        case self::PRESENTATION:
            $entityIdField = 'presentation_id';
            break;
        default:
            $entityIdField = 'conversion_task_id';
        }
        $entityKeywordIds = $keywordBindingTable->find()->where([$entityIdField => $entityId])->extract('keyword_id')->toArray();
        $newKeywordBindings = array();
        $keywordBindingsForSF = array();
        $keywords = $keywordTable->find();
        $keywordMap = array();
        foreach ($keywords as $k) {
            $keywordMap[$k->id] = $k->sf_id;
        }
        $courseTable = TableRegistry::get('Course');
        $courses = $courseTable->find();
        $courseMap = array();
        foreach ($courses as $c) {
            $courseMap[$c->presentation_id] = $c->sf_id;
        }
        $activityTable = TableRegistry::get('Activity');
        $activities = $activityTable->find()
            ->join([
                'table' => 'media',
                'alias' => 'm',
                'type' => 'LEFT',
                'conditions' => 'm.id=Activity.media_id AND (Activity.link=m.path OR Activity.link=m.first_slide)'
            ]);
        $activityMap = array();
        foreach ($activities as $a) {
            $activityMap[$a->media_id] = $a->sf_id;
        }
        foreach ($existingKeywordIds as $kwId) {
            if (!in_array($kwId, $entityKeywordIds)) {
                $keywordBinding = $keywordBindingTable->newEntity([$entityIdField => $entityId, 'keyword_id' => $kwId]);
                $newKeywordBindings[] = $keywordBinding;
                if ($entityType == self::PRESENTATION) {
                    if ($courseMap[$entityId] && $keywordMap[$kwId]) {
                        $kwsf = ['keyword' => $keywordMap[$kwId], 'presentation' => $courseMap[$entityId]];
                        $keywordBindingsForSF[] = $kwsf;
                    }
                } elseif ($entityType == self::MEDIA) {
                    if ($activityMap[$entityId] && $keywordMap[$kwId]) {
                        $kwsf = ['keyword' => $keywordMap[$kwId], 'activity' => $activityMap[$entityId]];
                        $keywordBindingsForSF[] = $kwsf;
                    }
                }
            }
        }
        $sf_ids = $this->Salesforce->addKeywordBinding($keywordBindingsForSF);
        foreach ($newKeywordBindings as $index => $kb){
            if($sf_ids[$index] && $sf_ids[$index]->success) {
                $kb->sf_id = $sf_ids[$index]->id;
            }
        }
        $keywordBindingTable->saveMany($newKeywordBindings);
    }

    private function deleteKeywordList($list, $entityId, $entityType) {
        $keywordTable = TableRegistry::get('Keyword');
        $keywordBindingTable = TableRegistry::get('KeywordBinding');
        switch ($entityType) {
        case self::MEDIA:
            $entityIdField = 'media_id';
            break;
        case self::PRESENTATION:
            $entityIdField = 'presentation_id';
            break;
        default:
            $entityIdField = 'conversion_task_id';
        }
        $keywordIds = $keywordTable->find()->where(['name IN' => $list])->extract('id')->toArray();
        $kbForSF = $keywordBindingTable->find()->where([$entityIdField => $entityId, 'keyword_id IN' => $keywordIds, 'sf_id IS NOT NULL'])->extract('sf_id')->toArray();
        $result = $this->Salesforce->setKeywordBindingInactive($kbForSF);
        $keywordBindingTable->deleteAll([$entityIdField => $entityId, 'keyword_id IN' => $keywordIds]);
    }
}