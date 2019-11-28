//
//  ModelManager.swift
//  CoreDataModelManager
//
//  Created by Johannes Dörr on 05.03.18.
//  Copyright © 2018 Johannes Dörr. All rights reserved.
//

import Foundation
import CoreData

public protocol ModelManagerDelegate: AnyObject {
    func modelManagerDidFinishLoading(_ modelManager: ModelManager, didClearDatabase: Bool)
    func modelManagerDidFailLoading(_ modelManager: ModelManager, error: Error)
}

open class ModelManager: NSObject {
    public weak var delegate: ModelManagerDelegate?
    public var isLoaded = false

    private let persistentContainer: NSPersistentContainer

    public let persistentStoreURL: URL
    
    public enum Error: Swift.Error {
        case notRequiredModelVersion
    }
    
    public struct InterProcessSync {
        /**
         The user defaults store to use for syncing. Should be a shared store using an app group.
         */
        let store: UserDefaults
        
        /**
         The key to use within the store for the local process
         */
        let localKey: String
        
        /**
         The keys that are used in the store for other process, that should be synced
         */
        let remoteKeys: [String]
        
        /**
         - Parameter store: The user defaults store to use for syncing. Should be a shared store using an app group.
         - Parameter localKey: The key to use within the store for the local process
         - Parameter remoteKeys: The keys that are used in the store for other process, that should be synced
         */
        public init(store: UserDefaults, localKey: String, remoteKeys: [String]) {
            self.store = store
            self.localKey = localKey
            self.remoteKeys = remoteKeys
        }
    }

    public let interProcessSync: InterProcessSync?
    let interProcessSyncQueue = DispatchQueue(label: "de.johannesdoerr.CoreDataModelManager.queue-\(UUID().uuidString)")
    
    /**
     The keys in store that this process will receive data on
     */
    var inboundKeys: [String] {
        guard let interProcessSync = self.interProcessSync else { return [] }
        return interProcessSync.remoteKeys.map { "\($0)->\(interProcessSync.localKey)" }
    }

    /**
     The keys in store that this process will send data to
     */
    var activeOutboundKeys: [String] {
        guard let interProcessSync = self.interProcessSync else { return [] }
        return interProcessSync.remoteKeys.compactMap { (outboundKey) in
            if interProcessSync.store.bool(forKey: outboundKey) {
                return "\(interProcessSync.localKey)->\(outboundKey)"
            } else {
                return nil
            }
        }
    }
    
    var applicationWillTerminateObserver: NSObjectProtocol?
    
    /**
     Creates a new instance of ModelManager
     - Parameter modelName: The name of the model file
     - Parameter modelBundle: The bundle of the model file
     - Parameter containerName: The name of the container
     - Parameter persistentStoreURL: The url where to store the database. If `nil`, it will be stored in the default directory using `containerName` as filename.
     - Parameter interProcessSync: Information about syncing changes of the contexts to other processes.
     */
    public init(modelName: String = "Model", modelBundle: Bundle = Bundle.main, containerName: String = "Model", persistentStoreURL: URL? = nil, interProcessSync: InterProcessSync? = nil) {
        guard let modelURL = modelBundle.url(forResource: modelName, withExtension: "momd"),
            let objectModel = NSManagedObjectModel(contentsOf: modelURL) else {
                fatalError("Failed to load Core Data stack")
        }
        self.persistentStoreURL = persistentStoreURL ?? NSPersistentContainer.defaultDirectoryURL().appendingPathComponent("\(containerName).sqlite")
        self.interProcessSync = interProcessSync
        persistentContainer = NSPersistentContainer(name: containerName, managedObjectModel: objectModel)
        let description = NSPersistentStoreDescription()
        description.shouldInferMappingModelAutomatically = true
        description.shouldMigrateStoreAutomatically = true
        description.url = self.persistentStoreURL
        persistentContainer.persistentStoreDescriptions = [description]
        super.init()
        
        if let interProcessSync = interProcessSync {
            // Mark local process as "observing":
            interProcessSync.store.set(true, forKey: interProcessSync.localKey)
            for inboundKey in inboundKeys {
                // Flush any pre-existing data:
                interProcessSync.store.set(nil, forKey: inboundKey)
                // Observe incoming changes:
                interProcessSync.store.addObserver(self, forKeyPath: inboundKey, options: [.old, .new], context: nil)
            }
        }
        
        applicationWillTerminateObserver = NotificationCenter.default.addObserver(forName: UIApplication.willTerminateNotification, object: nil, queue: nil) { [weak self] _ in
            // Mark local process as not "observing":
            if let interProcessSync = self?.interProcessSync {
                interProcessSync.store.set(nil, forKey: interProcessSync.localKey)
            }
        }
    }
    
    public enum ClearDatabase {
        case no
        case yes
        case yesIfMigrationFails
    }
    
    public enum LoadingResult {
        case success(didClearDatabase: Bool)
        case failure(Swift.Error)
    }
    
    /**
     Loads data from the file system
     - Parameter requiredModelVersionIdentifier: The identifier that the stored data must have.
     - Parameter clearDatabase: Options for purging data before loading
     - Parameter completion: The block to execute when the data has been loaded
     */
    open func load(requiredModelVersionIdentifier: String? = nil, clearDatabase: ClearDatabase = .no, completion: ((LoadingResult) ->())? = nil) {
        let requiredVersionFulfilled: Bool
        if let requiredIdentifier = requiredModelVersionIdentifier {
            if let metadata = try? NSPersistentStoreCoordinator.metadataForPersistentStore(ofType: "", at: persistentStoreURL, options: nil),
                let _identifiers = metadata[NSStoreModelVersionIdentifiersKey],
                let identifiers = _identifiers as? [String] {
                requiredVersionFulfilled = identifiers.contains(requiredIdentifier)
            } else {
                requiredVersionFulfilled = false
            }
        } else {
            requiredVersionFulfilled = true
        }
        let didClearDatabase: Bool
        switch clearDatabase {
        case .no where !requiredVersionFulfilled:
            let error = Error.notRequiredModelVersion
            self.delegate?.modelManagerDidFailLoading(self, error: error)
            completion?(.failure(error))
            return
        case .yes,
             .yesIfMigrationFails where !requiredVersionFulfilled:
            self.clearDatabase()
            didClearDatabase = true
        case .no, .yesIfMigrationFails:
            didClearDatabase = false
        }
        persistentContainer.loadPersistentStores { (description, error) in
            DispatchQueue.main.async {
                func succeeded(didClearDatabase: Bool) {
                    self.isLoaded = true
                    self.persistentContainer.viewContext.automaticallyMergesChangesFromParent = true
                    self.observeContextSaves(context: self.persistentContainer.viewContext)
                    self.delegate?.modelManagerDidFinishLoading(self, didClearDatabase: didClearDatabase)
                    completion?(.success(didClearDatabase: didClearDatabase))
                }
                func failed(with error: Swift.Error) {
                    self.isLoaded = false
                    self.delegate?.modelManagerDidFailLoading(self, error: error)
                    completion?(.failure(error))
                }
                if let error = error {
                    switch clearDatabase {
                    case .yesIfMigrationFails:
                        self.clearDatabase()
                        self.persistentContainer.loadPersistentStores { (description, error) in
                            DispatchQueue.main.async {
                                if let error = error {
                                    failed(with: error)
                                } else {
                                    succeeded(didClearDatabase: true)
                                }
                            }
                        }
                    case .no, .yes:
                        failed(with: error)
                    }
                } else {
                    succeeded(didClearDatabase: didClearDatabase)
                }
            }
        }
    }

    func clearDatabase() {
        do {
            try persistentContainer.persistentStoreCoordinator.destroyPersistentStore(at: persistentStoreURL, ofType: NSSQLiteStoreType, options: nil)
        } catch {
            print(error)
        }
    }

    open var viewContext: NSManagedObjectContext {
        return persistentContainer.viewContext
    }

    private var backgroundContexts = NSHashTable<NSManagedObjectContext>.weakObjects()

    let backgroundContextsAccessQueue = DispatchQueue(label: "de.johannesdoerr.CoreDataModelManager.queue-\(UUID().uuidString)")
    
    open func newBackgroundContext() -> NSManagedObjectContext {
        let context = persistentContainer.newBackgroundContext()
        backgroundContextsAccessQueue.sync {
            backgroundContexts.add(context)
        }
        self.observeContextSaves(context: context)
        return context
    }

    open func performBackgroundTask(_ block: @escaping (NSManagedObjectContext) -> Void) {
        persistentContainer.performBackgroundTask { context in
            self.backgroundContextsAccessQueue.sync {
                self.backgroundContexts.add(context)
            }
            block(context)
        }
    }

    open func mergeChangesToTopLevelContexts(fromRemoteContextSave changeNotificationData: [AnyHashable: Any]) {
        let topLevelContexts = self.backgroundContextsAccessQueue.sync {
            return backgroundContexts.allObjects + [viewContext]
        }
        let mergingContexts = topLevelContexts.filter { $0.automaticallyMergesChangesFromParent }
        NSManagedObjectContext.mergeChanges(fromRemoteContextSave: changeNotificationData, into: mergingContexts)
    }
    
    open func performBatchDeleteAndMergeChangesToTopLevelContexts(fetchRequest: NSFetchRequest<NSFetchRequestResult>, inContext context: NSManagedObjectContext) throws {
        let batchDeleteRequest = NSBatchDeleteRequest(fetchRequest: fetchRequest)
        batchDeleteRequest.resultType = .resultTypeObjectIDs
        let result = try context.execute(batchDeleteRequest) as? NSBatchDeleteResult
        let objectIDArray = result?.result as? [NSManagedObjectID]
        let changes = [NSDeletedObjectsKey: objectIDArray as Any]
        mergeChangesToTopLevelContexts(fromRemoteContextSave: changes)
    }

    open func managedObjectID(forURIRepresentation url: URL) -> NSManagedObjectID? {
        if url.scheme == "x-coredata" {
            return persistentContainer.persistentStoreCoordinator.managedObjectID(forURIRepresentation: url)
        } else {
            return nil
        }
    }
    
    func observeContextSaves(context: NSManagedObjectContext) {
        if let _ = interProcessSync {
            NotificationCenter.default.addObserver(self, selector: #selector(contextDidSave(notification:)), name: .NSManagedObjectContextDidSave, object: context)
        }
    }
    
    @objc
    func contextDidSave(notification: NSNotification) {
        interProcessSyncQueue.async {
            // Sync data with other processes
            guard let interProcessSync = self.interProcessSync else { return }
            let activeOutboundKeys = self.activeOutboundKeys
            if activeOutboundKeys.count == 0 { return }
            guard let userInfo = notification.userInfo else { return }
            var notificationData = [AnyHashable: Any]()
            for (key, value) in userInfo {
                guard let value = value as? NSSet else { continue }
                var uriRepresentations = [Any]()
                for element in value {
                    guard let managedObject = element as? NSManagedObject else { continue }
                    uriRepresentations.append(managedObject.objectID.uriRepresentation())
                }
                notificationData[key] = uriRepresentations
            }
            let archivedNotificationData: Data = NSKeyedArchiver.archivedData(withRootObject: notificationData)
            for outboundKey in activeOutboundKeys {
                var notificationDataList = [Any]()
                if let existingNotificationDatas = interProcessSync.store.array(forKey: outboundKey) {
                    notificationDataList.append(contentsOf: existingNotificationDatas)
                }
                notificationDataList.append(archivedNotificationData)
                interProcessSync.store.set(notificationDataList, forKey: outboundKey)
                interProcessSync.store.synchronize()
            }
        }
    }
    
    open override func observeValue(forKeyPath keyPath: String?, of object: Any?, change: [NSKeyValueChangeKey : Any]?, context: UnsafeMutableRawPointer?) {
        if let keyPath = keyPath, inboundKeys.contains(keyPath) {
            remoteContextDidSave(inboundKey: keyPath)
        }
    }
    
    func remoteContextDidSave(inboundKey: String) {
        // Incorporate remote changes
        guard let interProcessSync = self.interProcessSync else { return }
        guard let notificationDataList = interProcessSync.store.array(forKey: inboundKey) else {
            return
        }
        interProcessSync.store.removeObject(forKey: inboundKey)
        interProcessSync.store.synchronize()
        for _archievedNotificationData in notificationDataList {
            guard let archievedNotificationData = _archievedNotificationData as? Data,
                let notificationData = NSKeyedUnarchiver.unarchiveObject(with: archievedNotificationData) as? [NSObject : AnyObject]
            else { return }
            mergeChangesToTopLevelContexts(fromRemoteContextSave: notificationData)
        }
    }
}
