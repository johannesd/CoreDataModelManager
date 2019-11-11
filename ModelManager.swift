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

    private var databaseURL: URL {
        return NSPersistentContainer.defaultDirectoryURL().appendingPathComponent("\(persistentContainer.name).sqlite")
    }

    public enum Error: Swift.Error {
        case notRequiredModelVersion
    }

    /**
     Creates a new instance of ModelManager
     - Parameter modelName: The name of the model file
     - Parameter containerName: The name of the container
     */
    public init(withModelName modelName: String = "Model", bundle: Bundle = Bundle.main, containerName: String = "Model") {
        guard let modelURL = bundle.url(forResource: modelName, withExtension: "momd"),
            let objectModel = NSManagedObjectModel(contentsOf: modelURL) else {
                fatalError("Failed to load Core Data stack")
        }
        persistentContainer = NSPersistentContainer(name: containerName, managedObjectModel: objectModel)
        super.init()
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
    
    open func load(requiredModelVersionIdentifier: String? = nil, clearDatabase: ClearDatabase = .no, completion: ((LoadingResult) ->())? = nil) {
        let requiredVersionFulfilled: Bool
        if let requiredIdentifier = requiredModelVersionIdentifier {
            if let metadata = try? NSPersistentStoreCoordinator.metadataForPersistentStore(ofType: "", at: databaseURL, options: nil),
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
            try persistentContainer.persistentStoreCoordinator.destroyPersistentStore(at: databaseURL, ofType: NSSQLiteStoreType, options: nil)
        } catch {
            print(error)
        }
    }

    open var viewContext: NSManagedObjectContext {
        return persistentContainer.viewContext
    }

    private var backgroundContexts = NSHashTable<NSManagedObjectContext>.weakObjects()

    open func newBackgroundContext() -> NSManagedObjectContext {
        let context = persistentContainer.newBackgroundContext()
        backgroundContexts.add(context)
//        print("added context (\(self.backgroundContexts.count))")
        return context
    }

    open func performBackgroundTask(_ block: @escaping (NSManagedObjectContext) -> Void) {
        persistentContainer.performBackgroundTask { context in
            DispatchQueue.main.sync {
                self.backgroundContexts.add(context)
//                print("added context (\(self.backgroundContexts.count))")
            }
            block(context)
        }
    }

    open func mergeChangesToTopLevelContexts(fromRemoteContextSave changeNotificationData: [AnyHashable: Any]) {
        let topLevelContexts = backgroundContexts.allObjects + [viewContext]
        let mergingContexts = topLevelContexts.filter { $0.automaticallyMergesChangesFromParent }
        NSManagedObjectContext.mergeChanges(fromRemoteContextSave: changeNotificationData, into: mergingContexts)
    }

    open func managedObjectID(forURIRepresentation url: URL) -> NSManagedObjectID? {
        if url.scheme == "x-coredata" {
            return persistentContainer.persistentStoreCoordinator.managedObjectID(forURIRepresentation: url)
        } else {
            return nil
        }
    }
}
